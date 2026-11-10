package ai.pipestream.annbench;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

/**
 * Stock Lucene HNSW runner for the #53 ANN gate.
 *
 * <p>Reads a fixture directory written by {@code ann_bench fixture}, sweeps
 * Lucene99HnswVectorsFormat over the same M, beamWidth (ef_construction) and
 * query breadth grid, and writes a {@code protomolt-ann-bench-results-v1}
 * results file. The query breadth is KnnFloatVectorQuery's k (candidates);
 * the top k of those are kept, as luceneutil's fanout does.
 *
 * <p>Filters are Lucene's own: {@code IntField.newSetQuery} on the bucket or
 * cell field, passed as the KNN query's filter, so filter evaluation is
 * inside the timed region and Lucene may choose its exact path for
 * selective filters, as it does in production. The query cache is off so a
 * shared filter is not cached across queries.
 *
 * <p>Shapes: single (indexed by every core, then forceMerge(1)) or sealed
 * segments of --segment-rows plus a tail (one thread, flushed per segment,
 * NoMergePolicy), the multi-segment index #53 asks for.
 *
 * <p>Timing is a warmed loop (JIT warm-up pass per build, a recall pass per
 * point, then timed passes of at least --min-time-ms), not JMH: the sweep
 * needs each point's recall, which a JMH fork does not return.
 */
public final class LuceneRunner {
  private static final String FORMAT = "protomolt-ann-bench-results-v1";

  record Filter(String name, String column, int[] shared, int[][] perQuery) {
    int[] values(int q) {
      return shared != null ? shared : perQuery[q];
    }
  }

  static Map<String, String> args(String[] argv) {
    Map<String, String> m = new HashMap<>();
    for (String a : argv) {
      if (a.startsWith("--") && a.contains("=")) {
        int i = a.indexOf('=');
        m.put(a.substring(2, i), a.substring(i + 1));
      }
    }
    return m;
  }

  static int[] ints(String s) {
    return Arrays.stream(s.split(",")).filter(x -> !x.isBlank()).mapToInt(x -> Integer.parseInt(x.trim()))
        .sorted().distinct().toArray();
  }

  static ByteBuffer read(Path p) throws IOException {
    return ByteBuffer.wrap(Files.readAllBytes(p)).order(ByteOrder.LITTLE_ENDIAN);
  }

  static float[][] matrix(Path p, int rows, int dim) throws IOException {
    ByteBuffer b = read(p);
    float[][] out = new float[rows][dim];
    for (int r = 0; r < rows; r++) {
      for (int d = 0; d < dim; d++) {
        out[r][d] = b.getFloat();
      }
    }
    return out;
  }

  static int[] u32(Path p) throws IOException {
    ByteBuffer b = read(p);
    int[] out = new int[b.remaining() / 4];
    for (int i = 0; i < out.length; i++) {
      out[i] = b.getInt();
    }
    return out;
  }

  static int[] toInts(JsonNode n) {
    int[] out = new int[n.size()];
    for (int i = 0; i < out.length; i++) {
      out[i] = n.get(i).asInt();
    }
    return out;
  }

  public static void main(String[] argv) throws Exception {
    Map<String, String> a = args(argv);
    Path dir = Path.of(a.getOrDefault("fixture", ""));
    Path out = Path.of(a.getOrDefault("out", "lucene.json"));
    int[] ms = ints(a.getOrDefault("m", "16,32,48,64"));
    int[] efcs = ints(a.getOrDefault("ef-construction", "100,200,400"));
    int[] efs = ints(a.getOrDefault("ef", "16,32,48,64,96,128,192,256,384,512"));
    int[] ks = ints(a.getOrDefault("k", "10,100"));
    int segmentRows = Integer.parseInt(a.getOrDefault("segment-rows", "0"));
    long minTimeNs = Long.parseLong(a.getOrDefault("min-time-ms", "300")) * 1_000_000L;
    int threads = Integer.parseInt(a.getOrDefault("threads", "" + Runtime.getRuntime().availableProcessors()));
    double recallStop = Double.parseDouble(a.getOrDefault("recall-stop", "0.99"));
    Set<String> only = new HashSet<>(Arrays.asList(a.getOrDefault("filters", "").split(",")));
    only.remove("");
    Path work = Path.of(a.getOrDefault("work", System.getProperty("java.io.tmpdir"))).resolve("ann-bench-lucene");

    ObjectMapper json = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    JsonNode manifest = json.readTree(dir.resolve("manifest.json").toFile());
    if (!"protomolt-ann-fixture-v1".equals(manifest.get("format").asText())) {
      throw new IllegalArgumentException(dir + " is not a protomolt-ann-fixture-v1 fixture");
    }
    int n = manifest.get("rows").asInt();
    int dim = manifest.get("dim").asInt();
    int qn = manifest.get("queries").asInt();
    int kTruth = manifest.get("k_truth").asInt();
    float[][] base = matrix(dir.resolve("base.f32"), n, dim);
    float[][] queries = matrix(dir.resolve("queries.f32"), qn, dim);
    byte[] bucket = Files.readAllBytes(dir.resolve("bucket.u8"));
    ByteBuffer cellBuf = read(dir.resolve("cell.u16"));
    int[] cell = new int[n];
    for (int i = 0; i < n; i++) {
      cell[i] = Short.toUnsignedInt(cellBuf.getShort());
    }
    List<Filter> filters = new ArrayList<>();
    Map<String, int[]> truth = new HashMap<>();
    filters.add(new Filter("none", null, null, null));
    truth.put("none", u32(dir.resolve("truth/none.u32")));
    for (JsonNode f : manifest.get("filters")) {
      String name = f.get("name").asText();
      JsonNode body = json.readTree(dir.resolve("filters/" + name + ".json").toFile());
      int[] shared = body.has("shared") ? toInts(body.get("shared")) : null;
      int[][] per = null;
      if (shared == null) {
        JsonNode pq = body.get("per_query");
        per = new int[pq.size()][];
        for (int q = 0; q < per.length; q++) {
          per[q] = toInts(pq.get(q));
        }
      }
      filters.add(new Filter(name, body.get("column").asText(), shared, per));
      truth.put(name, u32(dir.resolve("truth/" + name + ".u32")));
    }
    if (!only.isEmpty()) {
      filters.removeIf(f -> !only.contains(f.name()));
    }

    List<int[]> ranges = new ArrayList<>();
    String shapeName = "single";
    if (segmentRows > 0 && segmentRows < n) {
      int sealed = 0;
      int tail = 0;
      for (int o = 0; o < n; o += segmentRows) {
        int len = Math.min(segmentRows, n - o);
        ranges.add(new int[] {o, len});
        if (len == segmentRows) {
          sealed++;
        } else {
          tail += len;
        }
      }
      shapeName = "segmented:" + sealed + "x" + segmentRows + "+tail" + tail;
    } else {
      ranges.add(new int[] {0, n});
    }

    Map<String, Object> report = new LinkedHashMap<>();
    report.put("format", FORMAT);
    Map<String, Object> fixture = new LinkedHashMap<>();
    fixture.put("name", manifest.get("name").asText());
    fixture.put("dir", dir.toString());
    fixture.put("rows", n);
    fixture.put("dim", dim);
    fixture.put("queries", qn);
    fixture.put("base_sha256", manifest.get("base_sha256").asText());
    fixture.put("filters", manifest.get("filters"));
    report.put("fixture", fixture);
    report.put("shape", shapeName);
    report.put("segments", ranges);
    report.put("host", host());
    report.put("harness", Map.of("runner", "lucene/LuceneRunner", "java", System.getProperty("java.version"),
        "commit", System.getenv().getOrDefault("ANN_BENCH_COMMIT", ""),
        "started_at", System.currentTimeMillis() / 1000L));
    report.put("settings", Map.of("k", ks, "ef", efs, "m", ms, "ef_construction", efcs,
        "min_time_ms", minTimeNs / 1_000_000L, "threads", threads, "recall_stop", recallStop));
    Map<String, Object> engine = new LinkedHashMap<>();
    engine.put("engine", "lucene-hnsw");
    engine.put("library", "Lucene99HnswVectorsFormat (Lucene104Codec), DOT_PRODUCT, KnnFloatVectorQuery");
    engine.put("version", Version.LATEST.toString());
    engine.put("license", "Apache-2.0");
    engine.put("filter", "IntField.newSetQuery filter on the KNN query (Lucene's own filter path)");
    List<Object> builds = new ArrayList<>();
    engine.put("builds", builds);
    report.put("engines", List.of(engine));

    ExecutorService pool = Executors.newFixedThreadPool(threads);
    ExecutorService mergePool = Executors.newFixedThreadPool(threads);
    for (int m : ms) {
      for (int efc : efcs) {
        Path idx = work.resolve("m" + m + "-efc" + efc);
        deleteTree(idx);
        Files.createDirectories(idx);
        long t0 = System.nanoTime();
        try (FSDirectory d = FSDirectory.open(idx)) {
          IndexWriterConfig iwc = new IndexWriterConfig();
          final int fm = m;
          final int fefc = efc;
          iwc.setCodec(new Lucene104Codec() {
            @Override
            public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
              return new Lucene99HnswVectorsFormat(fm, fefc, threads, mergePool);
            }
          });
          iwc.setRAMBufferSizeMB(4096);
          iwc.setUseCompoundFile(false);
          boolean segmented = ranges.size() > 1;
          if (segmented) {
            iwc.setMergePolicy(NoMergePolicy.INSTANCE);
          }
          try (IndexWriter w = new IndexWriter(d, iwc)) {
            if (segmented) {
              for (int[] r : ranges) {
                for (int row = r[0]; row < r[0] + r[1]; row++) {
                  w.addDocument(doc(base[row], bucket[row], cell[row], row));
                }
                w.flush();
              }
            } else {
              List<Future<?>> parts = new ArrayList<>();
              AtomicLong next = new AtomicLong();
              for (int t = 0; t < threads; t++) {
                parts.add(pool.submit(() -> {
                  for (long row = next.getAndIncrement(); row < n; row = next.getAndIncrement()) {
                    int r = (int) row;
                    w.addDocument(doc(base[r], bucket[r], cell[r], r));
                  }
                  return null;
                }));
              }
              for (Future<?> f : parts) {
                f.get();
              }
              w.forceMerge(1);
            }
            w.commit();
          }
          double buildS = (System.nanoTime() - t0) / 1e9;
          long disk = treeSize(idx);
          try (DirectoryReader reader = DirectoryReader.open(d)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            searcher.setQueryCache(null);
            int[] docToRow = new int[reader.maxDoc()];
            for (LeafReaderContext leaf : reader.leaves()) {
              NumericDocValues rows = leaf.reader().getNumericDocValues("row");
              for (int doc = 0; doc < leaf.reader().maxDoc(); doc++) {
                rows.advanceExact(doc);
                docToRow[leaf.docBase + doc] = (int) rows.longValue();
              }
            }
            System.err.printf("lucene: M=%d efc=%d built in %.1fs (%d segments)%n", m, efc, buildS,
                reader.leaves().size());
            Bench bench = new Bench(searcher, docToRow, queries, threads, minTimeNs, kTruth);
            // JIT warm-up.
            for (int rep = 0; rep < 3; rep++) {
              for (int q = 0; q < qn; q++) {
                bench.search(q, 10, 64, null);
              }
            }
            List<Object> cells = new ArrayList<>();
            for (Filter f : filters) {
              for (int k : ks) {
                List<Object> points = new ArrayList<>();
                for (int ef : efs) {
                  if (ef < k) {
                    continue;
                  }
                  Map<String, Object> p = bench.measure(f, truth.get(f.name()), k, ef);
                  points.add(p);
                  if ((double) p.get("recall_mean") >= recallStop) {
                    break;
                  }
                  // Plateau: three more breadths added < 0.005 recall.
                  int np = points.size();
                  if (np >= 4 && (double) p.get("recall_mean")
                      - (double) ((Map<?, ?>) points.get(np - 4)).get("recall_mean") < 0.005) {
                    break;
                  }
                }
                System.err.printf("lucene: M=%d efc=%d %s k=%d: %d points%n", m, efc, f.name(), k, points.size());
                cells.add(Map.of("filter", f.name(), "k", k, "points", points));
              }
            }
            Map<String, Object> b = new LinkedHashMap<>();
            b.put("params", Map.of("m", m, "ef_construction", efc, "bits", 0));
            b.put("build_s", buildS);
            b.put("retained_rss_bytes", null);
            b.put("retained_bytes_per_row", null);
            b.put("disk_bytes", disk);
            b.put("disk_bytes_per_row", (double) disk / n);
            b.put("segments_in_index", reader.leaves().size());
            b.put("cells", cells);
            builds.add(b);
            json.writeValue(out.toFile(), report);
          }
        }
        deleteTree(idx);
      }
    }
    pool.shutdown();
    mergePool.shutdown();
    System.err.println("lucene: wrote " + out);
  }

  static Document doc(float[] v, byte bucket, int cell, int row) {
    Document doc = new Document();
    doc.add(new KnnFloatVectorField("v", v, VectorSimilarityFunction.DOT_PRODUCT));
    doc.add(new IntField("bucket", Byte.toUnsignedInt(bucket), Field.Store.NO));
    doc.add(new IntField("cell", cell, Field.Store.NO));
    doc.add(new NumericDocValuesField("row", row));
    return doc;
  }

  static final class Bench {
    final IndexSearcher searcher;
    final int[] docToRow;
    final float[][] queries;
    final int threads;
    final long minTimeNs;
    final int kTruth;

    Bench(IndexSearcher searcher, int[] docToRow, float[][] queries, int threads, long minTimeNs, int kTruth) {
      this.searcher = searcher;
      this.docToRow = docToRow;
      this.queries = queries;
      this.threads = threads;
      this.minTimeNs = minTimeNs;
      this.kTruth = kTruth;
    }

    int[] search(int q, int k, int ef, Filter f) {
      try {
        Query filter = f == null || f.column() == null ? null : IntField.newSetQuery(f.column(), f.values(q));
        TopDocs td = searcher.search(new KnnFloatVectorQuery("v", queries[q], ef, filter), k);
        int[] rows = new int[td.scoreDocs.length];
        for (int i = 0; i < rows.length; i++) {
          rows[i] = docToRow[td.scoreDocs[i].doc];
        }
        return rows;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    double recall(int[] got, int[] truth, int q, int k) {
      Set<Integer> want = new HashSet<>();
      for (int i = 0; i < k; i++) {
        int t = truth[q * kTruth + i];
        if (t != -1) {
          want.add(t);
        }
      }
      if (want.isEmpty()) {
        return 1.0;
      }
      int hit = 0;
      for (int i = 0; i < Math.min(k, got.length); i++) {
        if (want.contains(got[i])) {
          hit++;
        }
      }
      return (double) hit / want.size();
    }

    Map<String, Object> measure(Filter f, int[] truth, int k, int ef) throws Exception {
      int qn = queries.length;
      double sum = 0;
      double worst = 1;
      for (int q = 0; q < qn; q++) {
        double r = recall(search(q, k, ef, f), truth, q, k);
        sum += r;
        worst = Math.min(worst, r);
      }
      List<Double> lat = new ArrayList<>();
      long done = 0;
      long t0 = System.nanoTime();
      int minQueries = Math.min(qn, 200);
      while (done < minQueries || System.nanoTime() - t0 < minTimeNs) {
        int q = (int) (done % qn);
        long a = System.nanoTime();
        search(q, k, ef, f);
        lat.add((System.nanoTime() - a) / 1e6);
        done++;
      }
      double oneCore = done / ((System.nanoTime() - t0) / 1e9);
      AtomicLong next = new AtomicLong();
      AtomicLong completed = new AtomicLong();
      long start = System.nanoTime();
      Thread[] ts = new Thread[threads];
      for (int t = 0; t < threads; t++) {
        ts[t] = new Thread(() -> {
          while (System.nanoTime() - start < minTimeNs) {
            int q = (int) (next.getAndIncrement() % qn);
            search(q, k, ef, f);
            completed.incrementAndGet();
          }
        });
        ts[t].start();
      }
      for (Thread t : ts) {
        t.join();
      }
      double allCores = completed.get() / ((System.nanoTime() - start) / 1e9);
      lat.sort(Comparator.naturalOrder());
      Map<String, Object> p = new LinkedHashMap<>();
      p.put("k", k);
      p.put("ef", ef);
      p.put("recall_mean", sum / qn);
      p.put("recall_worst", worst);
      p.put("qps_one_core", oneCore);
      p.put("qps_all_cores", allCores);
      p.put("threads", threads);
      p.put("p50_ms", lat.get((int) Math.round((lat.size() - 1) * 0.5)));
      p.put("p99_ms", lat.get((int) Math.round((lat.size() - 1) * 0.99)));
      p.put("predicate_ms_mean", null);
      p.put("hops_mean", null);
      p.put("exact_scores_mean", null);
      return p;
    }
  }

  static Map<String, Object> host() throws IOException {
    String cpu = "";
    Path info = Path.of("/proc/cpuinfo");
    if (Files.exists(info)) {
      for (String line : Files.readAllLines(info)) {
        if (line.startsWith("model name") || line.startsWith("Model")) {
          cpu = line.split(":", 2)[1].trim();
          break;
        }
      }
    }
    String name = System.getenv().getOrDefault("ANN_BENCH_HOST", InetAddress.getLocalHost().getHostName());
    Map<String, Object> h = new LinkedHashMap<>();
    h.put("name", name);
    h.put("cpu", cpu);
    h.put("arch", System.getProperty("os.arch"));
    h.put("kernel", System.getProperty("os.version"));
    h.put("threads", Runtime.getRuntime().availableProcessors());
    h.put("cpu_features", List.of());
    h.put("jvm", ManagementFactory.getRuntimeMXBean().getVmName() + " " + System.getProperty("java.version"));
    return h;
  }

  static long treeSize(Path p) throws IOException {
    try (var s = Files.walk(p)) {
      return s.filter(Files::isRegularFile).mapToLong(x -> x.toFile().length()).sum();
    }
  }

  static void deleteTree(Path p) throws IOException {
    if (!Files.exists(p)) {
      return;
    }
    try (var s = Files.walk(p)) {
      s.sorted(Comparator.reverseOrder()).forEach(x -> x.toFile().delete());
    }
  }
}
