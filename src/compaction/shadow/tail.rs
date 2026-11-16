//! WAL tail replay and live-WAL watermark fsync for online compaction.

use std::collections::BTreeMap;

use tonic::Status;

use crate::node::{NodeServiceImpl, ShardState};
use crate::pb::wal::{wal_record, LoggedAddDocuments, LoggedAddVectors};
use crate::pb::AddDocumentsRequest;

use super::super::{Analyze, Preflight, Shadow};

type RecordTextsBatch<'a> = (
    Vec<(
        &'a str,
        Option<&'a crate::pb::AnalysisSpec>,
        crate::analyzer::SessionLayers,
    )>,
    Vec<Vec<usize>>,
);

impl NodeServiceImpl {
    /// Batch-analyzes documents in `records` and applies the pass to `shadow` in clock order.
    pub(in crate::compaction) fn apply_pass(
        &self,
        shadow: &mut Shadow,
        records: Vec<crate::pb::wal::WalRecord>,
        analyze: &mut Analyze<'_>,
    ) -> Result<(), Status> {
        let docs: Vec<&AddDocumentsRequest> = records
            .iter()
            .filter_map(|record| match &record.op {
                Some(wal_record::Op::AddDocuments(add)) => Some(add.documents.iter()),
                _ => None,
            })
            .flatten()
            .collect();
        let mut analyzed = self.analyze_records(&docs, analyze)?.into_iter();
        for record in records {
            self.apply_to_shadow(shadow, record, &mut analyzed)?;
        }
        if analyzed.next().is_some() {
            return Err(Status::internal(
                "the tail analyzed more documents than it applied",
            ));
        }
        Ok(())
    }

    fn apply_tailed_vectors(
        &self,
        shadow: &mut Shadow,
        add: LoggedAddVectors,
    ) -> Result<(), Status> {
        let batch = add
            .batch
            .ok_or_else(|| Status::internal("WAL vector record has no batch"))?;
        let dim = batch.dim as usize;
        if dim == 0 || !batch.vectors.len().is_multiple_of(dim) {
            return Err(Status::internal("WAL vector record has invalid dimensions"));
        }
        let rows = (batch.vectors.len() / dim) as u64;
        let key = add.stable_routing_keys.into_iter().next();
        let (added, first) = self.apply_batch_locked(&mut shadow.state, batch, key)?;
        if added != rows {
            return Err(Status::internal(format!(
                "the shadow applied {added} of {rows} tailed vectors"
            )));
        }
        for i in 0..rows {
            map_tailed(&mut shadow.id_map, add.first_id + i, first + i)?;
        }
        Ok(())
    }

    fn apply_tailed_documents(
        &self,
        shadow: &mut Shadow,
        add: LoggedAddDocuments,
        analyzed: &mut std::vec::IntoIter<crate::postings::AnalyzedDoc>,
    ) -> Result<(), Status> {
        if add.stable_routing_keys.len() > add.documents.len() {
            return Err(Status::internal(
                "WAL document record carries more stable keys than documents",
            ));
        }
        let mut keys = add.stable_routing_keys.into_iter();
        for (i, doc) in add.documents.into_iter().enumerate() {
            let analyzed_doc = analyzed.next().ok_or_else(|| {
                Status::internal("the tail applied more documents than it analyzed")
            })?;
            let key = keys.next();
            let (doc, analyzed_doc) =
                self.materialize_document(doc, analyzed_doc, key.as_deref())?;
            let mut added = 0u64;
            let mut first = 0u64;
            self.apply_document_locked(
                &mut shadow.state,
                crate::node::MaterializedDocument {
                    doc,
                    analyzed: analyzed_doc,
                    vector: None,
                    stable_routing_key: key,
                },
                &mut added,
                &mut first,
            )?;
            map_tailed(&mut shadow.id_map, add.first_id + i as u64, first)?;
        }
        Ok(())
    }

    fn apply_to_shadow(
        &self,
        shadow: &mut Shadow,
        record: crate::pb::wal::WalRecord,
        analyzed: &mut std::vec::IntoIter<crate::postings::AnalyzedDoc>,
    ) -> Result<(), Status> {
        match record.op {
            Some(wal_record::Op::AddVectors(add)) => self.apply_tailed_vectors(shadow, add)?,
            Some(wal_record::Op::AddDocuments(add)) => {
                self.apply_tailed_documents(shadow, add, analyzed)?;
            }
            Some(wal_record::Op::DeleteDocument(delete)) => {
                let new_id = mapped(&shadow.id_map, delete.doc_id)?;
                let response = self.delete_documents_locked(&mut shadow.state, &[new_id], None)?;
                if response.deleted != 1 {
                    return Err(Status::internal(format!(
                        "tailed delete of source id {} (shadow id {new_id}) hit a row the shadow \
                         already tombstoned; the log and the shadow disagree",
                        delete.doc_id
                    )));
                }
            }
            Some(wal_record::Op::Replacement(replacement)) => {
                let old = mapped(&shadow.id_map, replacement.old_doc_id)?;
                let new = mapped(&shadow.id_map, replacement.new_doc_id)?;
                let response = self.commit_replacements_locked(
                    &mut shadow.state,
                    &[crate::pb::Replacement {
                        old_doc_id: old,
                        new_doc_id: new,
                    }],
                    None,
                )?;
                if response.committed != 1 {
                    return Err(Status::internal(format!(
                        "tailed replacement of source id {} hit a row the shadow already \
                         tombstoned; the log and the shadow disagree",
                        replacement.old_doc_id
                    )));
                }
            }
            Some(wal_record::Op::Bind(bind)) => {
                Self::apply_binding_locked(
                    &mut shadow.state,
                    crate::postings::StoredBinding {
                        plan_fingerprint: bind.plan_fingerprint,
                        body_path: bind.body_path,
                        materialize_sha: bind.materialize_sha,
                        analysis_sha: bind.analysis_sha,
                        analysis_contract: bind.analysis_contract,
                        vector_binding: bind.vector_binding,
                        index_contract: bind.index_contract,
                    },
                )?;
            }
            Some(wal_record::Op::Snapshot(snapshot)) => {
                return Err(Status::aborted(format!(
                    "a snapshot was installed on this shard during compaction (marker at clock \
                     {}, superseding generation {}); the compaction is aborted and the live \
                     shard is untouched",
                    record.clock, snapshot.source_generation
                )));
            }
            Some(wal_record::Op::Flush(_)) => return Ok(()),
            None => return Err(Status::internal("WAL record without an operation")),
        }
        shadow.tail_records += 1;
        if shadow.state.wal.is_none() {
            return Err(Status::internal(
                "the rewritten WAL generation failed to append and was retired; the compaction \
                 is aborted",
            ));
        }
        Ok(())
    }

    fn collect_record_texts<'a>(
        &self,
        docs: &[&'a AddDocumentsRequest],
    ) -> Result<RecordTextsBatch<'a>, Status> {
        let mut batch = Vec::new();
        let mut slots = Vec::with_capacity(docs.len());
        for doc in docs {
            batch.push((
                doc.text.as_str(),
                doc.analysis.as_ref(),
                crate::analyzer::SessionLayers {
                    sentences: !doc.sentence_fields.is_empty(),
                    dual_cased: !doc.cased_field.is_empty(),
                    ..Default::default()
                },
            ));
            let mut own = Vec::with_capacity(doc.fields.len());
            for field in &doc.fields {
                let fi = self
                    .config
                    .bm25_fields
                    .iter()
                    .position(|n| *n == field.field)
                    .ok_or_else(|| {
                        Status::failed_precondition(format!(
                            "logged document names field {:?}, which this node's table {:?} \
                             lacks",
                            field.field, self.config.bm25_fields
                        ))
                    })?;
                own.push(fi);
                batch.push((
                    field.text.as_str(),
                    field.analysis.as_ref(),
                    crate::analyzer::SessionLayers {
                        sentences: doc.sentence_fields.iter().any(|n| n == &field.field),
                        ..Default::default()
                    },
                ));
            }
            slots.push(own);
        }
        Ok((batch, slots))
    }

    fn analyze_records(
        &self,
        docs: &[&AddDocumentsRequest],
        analyze: &mut Analyze<'_>,
    ) -> Result<Vec<crate::postings::AnalyzedDoc>, Status> {
        if docs.is_empty() {
            return Ok(Vec::new());
        }
        let (batch, slots) = self.collect_record_texts(docs)?;
        let results = analyze(&batch)
            .map_err(|e| Status::unavailable(format!("analyze tailed documents: {e}")))?;
        if results.len() != batch.len() {
            return Err(Status::internal(format!(
                "analysis returned {} results for {} texts",
                results.len(),
                batch.len()
            )));
        }
        let mut results = results.into_iter();
        let mut out = Vec::with_capacity(docs.len());
        for (doc, own) in docs.iter().zip(slots) {
            let body = results.next().expect("counted above");
            let mut extras = Vec::with_capacity(own.len());
            for fi in own {
                let analyzed = results.next().expect("counted above");
                extras.push((fi, Some(analyzed.into_body())));
            }
            let cased =
                crate::node::cased_field_index(&self.config, self.phrase_index.as_deref(), doc)?;
            out.push(crate::node::join_fields(body, extras, cased)?);
        }
        Ok(out)
    }

    /// Fsyncs the live WAL under the shard write lock and returns its high watermark.
    pub(in crate::compaction) fn flush_live_wal(&self, pre: &Preflight) -> Result<u64, Status> {
        let mut guard = crate::node::write_shard(&self.state);
        Self::flush_live_wal_locked(&mut guard, pre)
    }

    /// Fsyncs the live WAL on a caller-held write guard, aborting if a snapshot rotated it.
    pub(in crate::compaction) fn flush_live_wal_locked(
        guard: &mut ShardState,
        pre: &Preflight,
    ) -> Result<u64, Status> {
        let wal = guard.wal.as_mut().ok_or_else(|| {
            Status::failed_precondition("the shard lost its WAL during compaction")
        })?;
        if wal.generation() != pre.cutoff_generation {
            return Err(Status::aborted(format!(
                "a snapshot was installed on this shard during compaction (the WAL rotated from \
                 generation {} to {}); the compaction is aborted and the live shard is untouched",
                pre.cutoff_generation,
                wal.generation()
            )));
        }
        wal.flush()
            .map_err(|e| Status::internal(format!("wal fsync during compaction: {e}")))?;
        Ok(wal.high_watermark())
    }
}

fn map_tailed(id_map: &mut BTreeMap<u64, u64>, old: u64, new: u64) -> Result<(), Status> {
    match id_map.insert(old, new) {
        None => Ok(()),
        Some(held) if held == new => Ok(()),
        Some(held) => Err(Status::internal(format!(
            "tailed source id {old} landed at shadow id {new} but its other leg landed at \
             {held}; the shadow's legs diverged"
        ))),
    }
}

fn mapped(id_map: &BTreeMap<u64, u64>, old: u64) -> Result<u64, Status> {
    id_map.get(&old).copied().ok_or_else(|| {
        Status::internal(format!(
            "the tail names source id {old}, which the compaction never saw as a live row; \
             the log and the dense image disagree"
        ))
    })
}
