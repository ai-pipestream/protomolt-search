//! The shared machinery of every composed unary route: opening the
//! children under one map reading, one request per child, the answers in
//! child or caller order, and the read receipt over the children's.

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use tonic::{Request, Response, Status};

use crate::coordinator::CoordinatorServiceImpl;
use crate::pb::{HealthRequest, HealthResponse};
use crate::stats_identity::StatsClaim;

use super::health::merge_health;
use super::map::{MapSnapshot, TokenTuple};
use super::stream_search::child_error;
use super::RelayService;

/// The children a composed route was opened over: the map reading they
/// were pinned under, a coordinator frozen over them, their addresses,
/// and the parent's claim translated per child.
pub(super) struct Opened {
    pub(super) pinned: MapSnapshot,
    pub(super) frozen: CoordinatorServiceImpl,
    pub(super) children: Vec<String>,
    pub(super) claims: Vec<StatsClaim>,
}

/// One request per child (`None` asks that child nothing), the answers
/// paired with their child index in child order; a child's error is
/// named by child and address.
pub(super) async fn fan_out<Req, Resp, F, Fut>(
    frozen: &CoordinatorServiceImpl,
    children: &[String],
    requests: Vec<Option<Req>>,
    timeout: Option<Duration>,
    route: &'static str,
    call: F,
) -> Result<Vec<(usize, Resp)>, Status>
where
    Req: Send + 'static,
    Resp: Send + 'static,
    F: Fn(crate::link::NodeLink, Request<Req>) -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<Output = Result<Response<Resp>, Status>> + Send + 'static,
{
    let mut tasks = Vec::with_capacity(children.len());
    for (shard, child_req) in requests.into_iter().enumerate() {
        let Some(child_req) = child_req else {
            continue;
        };
        let link = frozen.node_client(&children[shard])?;
        let addr = children[shard].clone();
        let call = call.clone();
        tasks.push((
            shard,
            tokio::spawn(async move {
                let mut request = Request::new(child_req);
                if let Some(timeout) = timeout {
                    request.set_timeout(timeout);
                }
                call(link, request)
                    .await
                    .map(|r| r.into_inner())
                    .map_err(|status| child_error(shard, &addr, route, status))
            }),
        ));
    }
    let mut out = Vec::with_capacity(tasks.len());
    for (shard, task) in tasks {
        let share = task
            .await
            .map_err(|e| Status::internal(format!("relay {route} task: {e}")))??;
        out.push((shard, share));
    }
    Ok(out)
}

/// The answers of a fan-out without their child indices.
pub(super) fn shares_only<Resp>(pairs: Vec<(usize, Resp)>) -> Vec<Resp> {
    pairs.into_iter().map(|(_, share)| share).collect()
}

/// The parent's request cloned once per child with that child's claim
/// written in by `set`.
pub(super) fn claimed_requests<Req: Clone>(
    req: &Req,
    claims: &[StatsClaim],
    set: impl Fn(&mut Req, &StatsClaim),
) -> Vec<Option<Req>> {
    claims
        .iter()
        .map(|claim| {
            let mut child_req = req.clone();
            set(&mut child_req, claim);
            Some(child_req)
        })
        .collect()
}

/// One request per child carrying that child's claim (written in by
/// `set`), fanned out over the children `opened` pinned; the answers in
/// child order.
pub(super) async fn fan_out_claimed<Req, Resp, F, Fut>(
    opened: &Opened,
    req: &Req,
    set: impl Fn(&mut Req, &StatsClaim),
    timeout: Option<Duration>,
    route: &'static str,
    call: F,
) -> Result<Vec<Resp>, Status>
where
    Req: Clone + Send + 'static,
    Resp: Send + 'static,
    F: Fn(crate::link::NodeLink, Request<Req>) -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<Output = Result<Response<Resp>, Status>> + Send + 'static,
{
    let requests = claimed_requests(req, &opened.claims, set);
    let pairs = fan_out(
        &opened.frozen,
        &opened.children,
        requests,
        timeout,
        route,
        call,
    )
    .await?;
    Ok(shares_only(pairs))
}

/// The children's visibility echoes checked against `scope`, their known
/// columns ORed into one list the relay answers with.
pub(super) fn echo_known<T>(
    scope: &crate::visibility::VisibilityScope,
    shares: &[T],
    echo: impl Fn(&T) -> (&[u8], &[bool]),
) -> Result<Vec<bool>, Status> {
    let mut known = vec![false; scope.column_count()];
    for share in shares {
        let (fingerprint, columns) = echo(share);
        scope.validate_echo(fingerprint, columns)?;
        super::merge_util::or_into(&mut known, columns);
    }
    Ok(known)
}

/// The parent's request sent unchanged to every child.
pub(super) fn same_requests<Req: Clone>(req: &Req, children: usize) -> Vec<Option<Req>> {
    (0..children).map(|_| Some(req.clone())).collect()
}

/// The children's answers put back in the caller's id order, each
/// occurrence of an id served once, the way a node answers a list that
/// repeats an id. An id no child answered is absent, as a node leaves
/// out an id it does not serve.
pub(super) fn in_caller_order<T>(ids: &[u64], items: Vec<T>, id_of: impl Fn(&T) -> u64) -> Vec<T> {
    let mut by_id: HashMap<u64, VecDeque<T>> = HashMap::new();
    for item in items {
        by_id.entry(id_of(&item)).or_default().push_back(item);
    }
    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        if let Some(item) = by_id.get_mut(id).and_then(VecDeque::pop_front) {
            out.push(item);
        }
    }
    out
}

/// A child's read receipt as the merge reads it.
pub(super) fn read_metadata(
    response: &impl crate::visibility::ScopedReadResponse,
) -> crate::pb::VectorReadReceipt {
    let view = response.read_view();
    crate::pb::VectorReadReceipt {
        vector_binding: None,
        stats_epoch: view.epoch,
        stats_incarnation: view.incarnation.to_vec(),
        visibility_fingerprint: view.fingerprint.to_vec(),
        visibility_columns_known: view.columns_known.to_vec(),
    }
}

impl RelayService {
    /// The current map reading, a coordinator frozen over its children,
    /// and their addresses; a relay with no children refuses.
    pub(super) fn pin_children(
        &self,
    ) -> Result<(MapSnapshot, CoordinatorServiceImpl, Vec<String>), Status> {
        let (pinned, frozen) = self.pin();
        let children = frozen.node_addresses().to_vec();
        if children.is_empty() {
            return Err(Status::failed_precondition(
                "relay: a relay coordinator has no children",
            ));
        }
        Ok((pinned, frozen, children))
    }

    /// The children, the map they were pinned under, and the parent's
    /// claim translated per child: the opening every composed unary
    /// route shares.
    pub(super) fn open_children(
        &self,
        route: &str,
        visibility: Option<&crate::pb::DocumentVisibility>,
        epoch: u64,
        incarnation: &[u8],
    ) -> Result<Opened, Status> {
        crate::visibility::VisibilityScope::new(visibility)?;
        let (pinned, frozen) = self.pin();
        let children = frozen.node_addresses().to_vec();
        if children.is_empty() {
            return Err(Status::failed_precondition(format!(
                "relay: a relay coordinator has no children ({route})"
            )));
        }
        let claims = self.child_claims(epoch, incarnation, children.len())?;
        Ok(Opened {
            pinned,
            frozen,
            children,
            claims,
        })
    }

    /// The slot range of each child under `frozen`, from their health.
    pub(super) async fn ranges_of(
        &self,
        frozen: &CoordinatorServiceImpl,
        children: &[String],
        timeout: Option<Duration>,
    ) -> Result<Vec<(u64, u64)>, Status> {
        let reports = self.children_health(frozen, timeout).await?;
        super::merge_util::child_ranges(&self.collection, children, &reports)
    }

    /// Every child's health under one map reading, in shard order; a
    /// child that does not answer fails the call by name.
    pub(super) async fn children_health(
        &self,
        frozen: &CoordinatorServiceImpl,
        timeout: Option<Duration>,
    ) -> Result<Vec<HealthResponse>, Status> {
        let children = frozen.node_addresses().to_vec();
        let requests = same_requests(&HealthRequest {}, children.len());
        let reports = fan_out(
            frozen,
            &children,
            requests,
            timeout,
            "health",
            |mut link, request| async move { link.health(request).await },
        )
        .await?;
        Ok(shares_only(reports))
    }

    /// The startup check and the `Health` body: the children's reports
    /// merged into the one shard the parent sees, refused by name when
    /// the children's slot ranges are not contiguous.
    pub async fn check_children(&self) -> Result<HealthResponse, Status> {
        let (pinned, frozen) = self.pin();
        let reports = self.children_health(&frozen, None).await?;
        let merged = merge_health(&self.collection, frozen.node_addresses(), &reports)?;
        self.still_current(&pinned, "Health")?;
        Ok(merged)
    }

    /// The relay's read receipt over its children's: every child's echo
    /// checked against the scope and the expected claims, the known
    /// columns ORed, and the epochs bound into a relay token.
    pub(super) fn read_receipt<R: crate::visibility::ScopedReadResponse>(
        &self,
        pinned: &MapSnapshot,
        children: Vec<String>,
        visibility: Option<&crate::pb::DocumentVisibility>,
        expected: Option<&[StatsClaim]>,
        shares: &[R],
    ) -> Result<crate::pb::VectorReadReceipt, Status> {
        let scope = crate::visibility::VisibilityScope::new(visibility)?;
        if shares.len() != children.len() || shares.is_empty() {
            return Err(Status::failed_precondition(
                "relay read receipt needs every child",
            ));
        }
        let mut known = vec![false; scope.column_count()];
        let mut epochs = Vec::with_capacity(shares.len());
        for (child, share) in shares.iter().enumerate() {
            let view = share.read_view();
            scope.validate_echo(view.fingerprint, view.columns_known)?;
            let claim = StatsClaim::required(view.epoch, view.incarnation)?;
            if expected.is_some_and(|claims| {
                claims
                    .get(child)
                    .is_none_or(|expected| expected.epoch != 0 && *expected != claim)
            }) {
                return Err(Status::failed_precondition(
                    "relay child read version changed",
                ));
            }
            super::merge_util::or_into(&mut known, view.columns_known);
            epochs.push(claim);
        }
        self.still_current(pinned, "read receipt")?;
        let stats_epoch = self.allocate_token(pinned, children, epochs)?;
        Ok(crate::pb::VectorReadReceipt {
            vector_binding: None,
            stats_epoch,
            stats_incarnation: self.stats_incarnation.bytes()?,
            visibility_fingerprint: scope.fingerprint().to_vec(),
            visibility_columns_known: known,
        })
    }

    /// A relay token bound to `children` and their `epochs` under `pinned`.
    pub(super) fn allocate_token(
        &self,
        pinned: &MapSnapshot,
        children: Vec<String>,
        epochs: Vec<StatsClaim>,
    ) -> Result<u64, Status> {
        self.tokens
            .lock()
            .expect("relay token registry poisoned")
            .allocate(TokenTuple {
                collection: self.collection.clone(),
                control_revision: pinned.control_revision,
                topology_generation: pinned.topology_generation,
                children,
                epochs,
            })
    }
}
