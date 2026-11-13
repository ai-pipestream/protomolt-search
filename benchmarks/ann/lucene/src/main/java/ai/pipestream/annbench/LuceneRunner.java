package ai.pipestream.annbench;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.io.InputStream;
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
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
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
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

/**
 * Lucene HNSW runner for the #53 ANN gate.
 *
 * <p>Reads a fixture directory written by {@code ann_bench fixture}, sweeps
 * Lucene99HnswVectorsFormat over the same M, beamWidth (ef_construction) and
 * query breadth grid, and writes a {@code protomolt-ann-bench-results-v1}
 * results file. The query breadth is KnnFloatVectorQuery's k (candidates);
 * the top k of those are kept, as luceneutil's fanout does.
 *
 * <p>The Lucene build is chosen when the jar is packaged: {@code
 * -Dlucene.version=} picks the lucene-core the runner compiles and runs
 * against, and {@code -Dlucene.engine=} names it in the results. Stock
 * Lucene is {@code lucene-hnsw}; any other version must carry another name,
 * such as {@code lucene-hnsw-16357} for a build of apache/lucene PR #16357
 * (the sandbox code for apache/lucene issue #16958), and the runner refuses
 * a mismatch. The runner drives the same stock query path on every build;
 * it does not use the shared-floor code of #16357 yet, so every result it
 * writes is a floor-off result.
 *
 * <p>Filters are Lucene's own: {@code IntField.newSetQuery} on the bucket or
 * cell field, passed as the KNN query's filter, so filter evaluation is
 * inside the timed region and Lucene may choose its exact path for
 * selective filters, as it does in production. The query cache is off so a
 * shared filter is not cached across queries.
 *
 * <p>Shapes: single (indexed by every core, then forceMerge(1)); sealed
 * segments of --segment-rows plus a tail (one thread, flushed per segment,
 * NoMergePolicy), the multi-segment index #53 asks for; or sharded
 * (--placement=hash|skewed --shards=N): one index per shard, each built as
 * the single shape is, from the placement file the fixture wrote. A
 * sharded query asks every shard for its top k, one shard after another,
 * and merges them to the global top k by score descending, then global row
 * ascending. Sharded runs measure the unfiltered cell only.
 *
 * <p>Timing is a warmed loop (JIT warm-up pass per build, a recall pass per
 * point, then timed passes of at least --min-time-ms), not JMH: the sweep
 * needs each point's recall, which a JMH fork does not return.
 */
public final class LuceneRunner {
  private static final String FORMAT = "protomolt-ann-bench-results-v1";
  private static final String FIXTURE_FORMAT = "protomolt-ann-fixture-v2";
  private static final String STOCK_ENGINE = "lucene-hnsw";
  private static final List<String> PLACEMENTS = List.of("hash", "skewed");

  record Filter(String name, String column, int[] shared, int[][] perQuery) {
    int[] values(int q) {
      return shared != null ? shared : perQuery[q];
    }
  }

  /** The Lucene build this jar was packaged for, and its name in results. */
  record Build(String engine, String mavenVersion, String stockVersion) {}

  /** One searchable index and the global row of each of its documents. */
  record Part(IndexSearcher searcher, int[] docToRow, int leaves) {}

  /** One shard's hit, by the score Lucene reported and its global row. */
  record Hit(float score, int row) {
    /** Score descending, then global row ascending. */
    static final Comparator<Hit> ORDER =
        Comparator.comparing(Hit::score, Comparator.reverseOrder()).thenComparingInt(Hit::row);
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

  /**
   * Reads the build identity Maven filtered into the jar and refuses a name
   * that does not match the version: stock Lucene is lucene-hnsw, and any
   * other version needs its own lucene-hnsw-NAME.
   */
  static Build build() throws IOException {
    Properties p = new Properties();
    try (InputStream in = LuceneRunner.class.getResourceAsStream("/annbench-lucene.properties")) {
      if (in == null) {
        throw new IllegalStateException("annbench-lucene.properties is missing from the jar; package it with mvn");
      }
      p.load(in);
    }
    String engine = p.getProperty("lucene.engine", "");
    String version = p.getProperty("lucene.version", "");
    String stock = p.getProperty("lucene.stock.version", "");
    if (version.isEmpty() || stock.isEmpty() || engine.isEmpty()) {
      throw new IllegalStateException("annbench-lucene.properties lacks lucene.engine, lucene.version or "
          + "lucene.stock.version: " + p);
    }
    if (version.equals(stock) && !engine.equals(STOCK_ENGINE)) {
      throw new IllegalArgumentException("Lucene " + version + " is the stock build, which is named "
          + STOCK_ENGINE + "; -Dlucene.engine=" + engine + " needs a -Dlucene.version other than " + stock);
    }
    if (!version.equals(stock) && !engine.startsWith(STOCK_ENGINE + "-")) {
      throw new IllegalArgumentException("Lucene " + version + " is not the stock " + stock
          + ": name the build with -Dlucene.engine=" + STOCK_ENGINE + "-NAME, such as "
          + STOCK_ENGINE + "-16357 for apache/lucene PR #16357");
    }
    return new Build(engine, version, stock);
  }

  public static void main(String[] argv) throws Exception {
    Map<String, String> a = args(argv);
    Build lucene = build();
    Path dir = Path.of(a.getOrDefault("fixture", ""));
    Path out = Path.of(a.getOrDefault("out", "lucene.json"));
    String placement = a.getOrDefault("placement", "");
    String shardsArg = a.getOrDefault("shards", "");
    int segmentRows = Integer.parseInt(a.getOrDefault("segment-rows", "0"));
    boolean sharded = !placement.isEmpty();
    int shards = 0;
    if (!sharded && !shardsArg.isEmpty()) {
      throw new IllegalArgumentException("--shards needs --placement=hash|skewed");
    }
    if (sharded) {
      if (!PLACEMENTS.contains(placement)) {
        throw new IllegalArgumentException("--placement=" + placement + " must be one of " + PLACEMENTS);
      }
      if (segmentRows != 0) {
        throw new IllegalArgumentException("--segment-rows and --placement name different shapes; give one of them");
      }
      shards = Integer.parseInt(shardsArg.isEmpty() ? "4" : shardsArg);
      if (shards < 2) {
        throw new IllegalArgumentException("--shards=" + shards + ": the sharded shape needs at least 2 shards");
      }
    }
    int[] ms = ints(a.getOrDefault("m", "16,32,48,64"));
    int[] efcs = ints(a.getOrDefault("ef-construction", "100,200,400"));
    int[] efs = ints(a.getOrDefault("ef", sharded
        ? "100,128,192,256,384,512,768,1000,1536,2048,3072,4096,6144,8192,10000,12288,16384,24576,32768"
        : "16,32,48,64,96,128,192,256,384,512"));
    int[] ks = ints(a.getOrDefault("k", sharded ? "100,1000,10000" : "10,100"));
    long minTimeNs = Long.parseLong(a.getOrDefault("min-time-ms", "300")) * 1_000_000L;
    int threads = Integer.parseInt(a.getOrDefault("threads", "" + Runtime.getRuntime().availableProcessors()));
    double recallStop = Double.parseDouble(a.getOrDefault("recall-stop", "0.99"));
    Set<String> only = new HashSet<>(Arrays.asList(a.getOrDefault("filters", "").split(",")));
    only.remove("");
    Path work = Path.of(a.getOrDefault("work", System.getProperty("java.io.tmpdir"))).resolve("ann-bench-lucene");

    ObjectMapper json = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    JsonNode manifest = json.readTree(dir.resolve("manifest.json").toFile());
    if (!FIXTURE_FORMAT.equals(manifest.get("format").asText())) {
      throw new IllegalArgumentException(dir + " is a " + manifest.get("format").asText() + " fixture, not "
          + FIXTURE_FORMAT + "; rebuild it with ann_bench fixture");
    }
    int n = manifest.get("rows").asInt();
    int dim = manifest.get("dim").asInt();
    int qn = manifest.get("queries").asInt();
    int kTruth = manifest.get("k_truth").asInt();
    int kTruthNone = manifest.get("k_truth_none").asInt();
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
    Map<String, Integer> depth = new HashMap<>();
    filters.add(new Filter("none", null, null, null));
    truth.put("none", u32(dir.resolve("truth/none.u32")));
    depth.put("none", kTruthNone);
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
      depth.put(name, kTruth);
    }
    for (String o : only) {
      if (!truth.containsKey(o)) {
        throw new IllegalArgumentException("--filters: " + o + " is not a filter of this fixture");
      }
    }
    if (sharded) {
      // A shard's documents do not follow the global rows a filter names,
      // and no per-shard filter is built yet: unfiltered only.
      if (only.stream().anyMatch(o -> !o.equals("none"))) {
        throw new IllegalArgumentException("the sharded shape measures the unfiltered cell only (--filters=none)");
      }
      filters.removeIf(f -> !f.name().equals("none"));
    } else if (!only.isEmpty()) {
      filters.removeIf(f -> !only.contains(f.name()));
    }
    for (Filter f : filters) {
      for (int k : ks) {
        if (k < 1) {
          throw new IllegalArgumentException("--k must list values of at least 1");
        }
        if (k > depth.get(f.name())) {
          throw new IllegalArgumentException("k=" + k + " exceeds the fixture's truth depth " + depth.get(f.name())
              + " for " + f.name());
        }
      }
    }
    for (int k : ks) {
      if (Arrays.stream(efs).noneMatch(ef -> ef >= k)) {
        throw new IllegalArgumentException("no query breadth of at least k=" + k + " in --ef; list one");
      }
    }

    List<int[]> ranges = new ArrayList<>();
    List<int[]> shardRows = new ArrayList<>();
    String shapeName = "single";
    Map<String, Object> shardInfo = null;
    if (sharded) {
      String file = placement + "-" + shards;
      boolean listed = false;
      for (JsonNode p : manifest.get("placements")) {
        listed |= p.get("name").asText().equals(file);
      }
      if (!listed) {
        throw new IllegalArgumentException(dir + ": no placement " + file
            + "; rebuild the fixture with --shard-counts listing " + shards);
      }
      ByteBuffer pb = read(dir.resolve("placements/" + file + ".u16"));
      int[] count = new int[shards];
      int[] shardOf = new int[n];
      for (int r = 0; r < n; r++) {
        shardOf[r] = Short.toUnsignedInt(pb.getShort());
        if (shardOf[r] >= shards) {
          throw new IllegalArgumentException("placements/" + file + ".u16 names shard " + shardOf[r] + " of " + shards);
        }
        count[shardOf[r]]++;
      }
      List<Integer> sizes = new ArrayList<>();
      for (int s = 0; s < shards; s++) {
        if (count[s] == 0) {
          throw new IllegalArgumentException("placement " + file + " leaves shard " + s + " empty");
        }
        shardRows.add(new int[count[s]]);
        sizes.add(count[s]);
      }
      int[] fill = new int[shards];
      for (int r = 0; r < n; r++) {
        shardRows.get(shardOf[r])[fill[shardOf[r]]++] = r;
      }
      shapeName = "sharded:" + shards + ":" + placement + ":floor-off";
      shardInfo = new LinkedHashMap<>();
      shardInfo.put("count", shards);
      shardInfo.put("placement", placement);
      shardInfo.put("floor", "off");
      shardInfo.put("rows_per_shard", sizes);
      shardInfo.put("fan_out", "in process, sequential within a query; each shard index returns its top k and "
          + "the merge keeps the global top k by score descending, then global row ascending");
    } else if (segmentRows > 0 && segmentRows < n) {
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
    if (sharded) {
      report.put("shards", shardInfo);
    } else {
      report.put("segments", ranges);
    }
    report.put("host", host());
    report.put("harness", Map.of("runner", "lucene/LuceneRunner", "java", System.getProperty("java.version"),
        "commit", System.getenv().getOrDefault("ANN_BENCH_COMMIT", ""),
        "started_at", System.currentTimeMillis() / 1000L));
    report.put("settings", Map.of("k", ks, "ef", efs, "m", ms, "ef_construction", efcs,
        "min_time_ms", minTimeNs / 1_000_000L, "threads", threads, "recall_stop", recallStop));
    Map<String, Object> engine = new LinkedHashMap<>();
    engine.put("engine", lucene.engine());
    engine.put("library", "Lucene99HnswVectorsFormat (default codec), DOT_PRODUCT, KnnFloatVectorQuery"
        + (lucene.engine().equals(STOCK_ENGINE) ? ""
            : "; lucene-core " + lucene.mavenVersion() + ", not the stock " + lucene.stockVersion()
                + ", its shared floor not used"));
    engine.put("version", Version.LATEST.toString() + " (maven " + lucene.mavenVersion() + ")");
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
        List<FSDirectory> dirs = new ArrayList<>();
        if (sharded) {
          for (int s = 0; s < shards; s++) {
            FSDirectory d = FSDirectory.open(idx.resolve("shard-" + s));
            dirs.add(d);
            writeMerged(d, codec(m, efc, threads, mergePool), pool, threads, base, bucket, cell, shardRows.get(s));
          }
        } else {
          FSDirectory d = FSDirectory.open(idx);
          dirs.add(d);
          if (ranges.size() > 1) {
            writeSegmented(d, codec(m, efc, threads, mergePool), base, bucket, cell, ranges);
          } else {
            int[] all = new int[n];
            Arrays.setAll(all, r -> r);
            writeMerged(d, codec(m, efc, threads, mergePool), pool, threads, base, bucket, cell, all);
          }
        }
        double buildS = (System.nanoTime() - t0) / 1e9;
        long disk = treeSize(idx);
        List<DirectoryReader> readers = new ArrayList<>();
        List<Part> parts = new ArrayList<>();
        int leaves = 0;
        for (FSDirectory d : dirs) {
          DirectoryReader reader = DirectoryReader.open(d);
          readers.add(reader);
          IndexSearcher searcher = new IndexSearcher(reader);
          searcher.setQueryCache(null);
          parts.add(new Part(searcher, docToRow(reader), reader.leaves().size()));
          leaves += reader.leaves().size();
        }
        System.err.printf("lucene: %s M=%d efc=%d built in %.1fs (%d indexes, %d segments)%n", lucene.engine(), m,
            efc, buildS, parts.size(), leaves);
        Bench bench = new Bench(parts, sharded, queries, threads, minTimeNs);
        // JIT warm-up.
        long[] scratch = new long[bench.partCount()];
        for (int rep = 0; rep < 3; rep++) {
          for (int q = 0; q < qn; q++) {
            bench.search(q, 10, 64, null, scratch);
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
              Map<String, Object> p = bench.measure(f, truth.get(f.name()), depth.get(f.name()), k, ef);
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
        b.put("segments_in_index", leaves);
        b.put("cells", cells);
        builds.add(b);
        json.writeValue(out.toFile(), report);
        for (DirectoryReader r : readers) {
          r.close();
        }
        for (FSDirectory d : dirs) {
          d.close();
        }
        deleteTree(idx);
      }
    }
    pool.shutdown();
    mergePool.shutdown();
    System.err.println("lucene: wrote " + out);
  }

  /**
   * The default codec with every vector field in Lucene99HnswVectorsFormat at
   * this M and beamWidth. Wrapping the default codec, rather than naming a
   * codec class, keeps the runner building against other Lucene versions.
   */
  static Codec codec(int m, int efc, int threads, ExecutorService mergePool) {
    Codec base = Codec.getDefault();
    KnnVectorsFormat hnsw = new Lucene99HnswVectorsFormat(m, efc, threads, mergePool);
    KnnVectorsFormat perField = new PerFieldKnnVectorsFormat() {
      @Override
      public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        return hnsw;
      }
    };
    return new FilterCodec(base.getName(), base) {
      @Override
      public KnnVectorsFormat knnVectorsFormat() {
        return perField;
      }
    };
  }

  static IndexWriterConfig config(Codec codec) {
    IndexWriterConfig iwc = new IndexWriterConfig();
    iwc.setCodec(codec);
    iwc.setRAMBufferSizeMB(4096);
    iwc.setUseCompoundFile(false);
    return iwc;
  }

  /** The given rows indexed by every core, then merged to one segment. */
  static void writeMerged(FSDirectory d, Codec codec, ExecutorService pool, int threads, float[][] base,
      byte[] bucket, int[] cell, int[] rows) throws Exception {
    try (IndexWriter w = new IndexWriter(d, config(codec))) {
      List<Future<?>> parts = new ArrayList<>();
      AtomicLong next = new AtomicLong();
      for (int t = 0; t < threads; t++) {
        parts.add(pool.submit(() -> {
          for (long i = next.getAndIncrement(); i < rows.length; i = next.getAndIncrement()) {
            int r = rows[(int) i];
            w.addDocument(doc(base[r], bucket[r], cell[r], r));
          }
          return null;
        }));
      }
      for (Future<?> f : parts) {
        f.get();
      }
      w.forceMerge(1);
      w.commit();
    }
  }

  /** Sealed segments plus a tail: one thread, flushed per range, never merged. */
  static void writeSegmented(FSDirectory d, Codec codec, float[][] base, byte[] bucket, int[] cell,
      List<int[]> ranges) throws IOException {
    IndexWriterConfig iwc = config(codec);
    iwc.setMergePolicy(NoMergePolicy.INSTANCE);
    try (IndexWriter w = new IndexWriter(d, iwc)) {
      for (int[] r : ranges) {
        for (int row = r[0]; row < r[0] + r[1]; row++) {
          w.addDocument(doc(base[row], bucket[row], cell[row], row));
        }
        w.flush();
      }
      w.commit();
    }
  }

  static int[] docToRow(DirectoryReader reader) throws IOException {
    int[] docToRow = new int[reader.maxDoc()];
    for (LeafReaderContext leaf : reader.leaves()) {
      NumericDocValues rows = leaf.reader().getNumericDocValues("row");
      for (int doc = 0; doc < leaf.reader().maxDoc(); doc++) {
        rows.advanceExact(doc);
        docToRow[leaf.docBase + doc] = (int) rows.longValue();
      }
    }
    return docToRow;
  }

  static Document doc(float[] v, byte bucket, int cell, int row) {
    Document doc = new Document();
    doc.add(new KnnFloatVectorField("v", v, VectorSimilarityFunction.DOT_PRODUCT));
    doc.add(new IntField("bucket", Byte.toUnsignedInt(bucket), Field.Store.NO));
    doc.add(new IntField("cell", cell, Field.Store.NO));
    doc.add(new NumericDocValuesField("row", row));
    return doc;
  }

  /**
   * A KNN query that records the vectors each segment's search visited:
   * Lucene reports a segment's visit count as the total hits of its
   * per-segment result, which this reads before the results are merged.
   */
  static final class CountingKnnQuery extends KnnFloatVectorQuery {
    long[] visitedPerLeaf = new long[0];

    CountingKnnQuery(String field, float[] target, int k, Query filter) {
      super(field, target, k, filter);
    }

    @Override
    protected TopDocs mergeLeafResults(TopDocs[] perLeafResults) {
      visitedPerLeaf = new long[perLeafResults.length];
      for (int i = 0; i < perLeafResults.length; i++) {
        visitedPerLeaf[i] = perLeafResults[i].totalHits.value();
      }
      return super.mergeLeafResults(perLeafResults);
    }
  }

  static final class Bench {
    final List<Part> parts;
    final boolean sharded;
    final float[][] queries;
    final int threads;
    final long minTimeNs;

    Bench(List<Part> parts, boolean sharded, float[][] queries, int threads, long minTimeNs) {
      this.parts = parts;
      this.sharded = sharded;
      this.queries = queries;
      this.threads = threads;
      this.minTimeNs = minTimeNs;
    }

    /** Shards when sharded, otherwise the one index's segments. */
    int partCount() {
      return sharded ? parts.size() : parts.get(0).leaves();
    }

    /** The top k global rows; `visited` gets each part's visit count. */
    int[] search(int q, int k, int ef, Filter f, long[] visited) {
      try {
        Query filter = f == null || f.column() == null ? null : IntField.newSetQuery(f.column(), f.values(q));
        if (!sharded) {
          Part part = parts.get(0);
          CountingKnnQuery query = new CountingKnnQuery("v", queries[q], ef, filter);
          TopDocs td = part.searcher().search(query, k);
          Arrays.fill(visited, 0);
          for (int i = 0; i < query.visitedPerLeaf.length && i < visited.length; i++) {
            visited[i] = query.visitedPerLeaf[i];
          }
          int[] rows = new int[td.scoreDocs.length];
          for (int i = 0; i < rows.length; i++) {
            rows[i] = part.docToRow()[td.scoreDocs[i].doc];
          }
          return rows;
        }
        List<Hit> hits = new ArrayList<>();
        for (int s = 0; s < parts.size(); s++) {
          Part part = parts.get(s);
          CountingKnnQuery query = new CountingKnnQuery("v", queries[q], ef, filter);
          TopDocs td = part.searcher().search(query, k);
          visited[s] = Arrays.stream(query.visitedPerLeaf).sum();
          for (ScoreDoc sd : td.scoreDocs) {
            hits.add(new Hit(sd.score, part.docToRow()[sd.doc]));
          }
        }
        hits.sort(Hit.ORDER);
        int[] rows = new int[Math.min(k, hits.size())];
        for (int i = 0; i < rows.length; i++) {
          rows[i] = hits.get(i).row();
        }
        return rows;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    double recall(int[] got, int[] truth, int depth, int q, int k) {
      Set<Integer> want = new HashSet<>();
      for (int i = 0; i < k; i++) {
        int t = truth[q * depth + i];
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

    static double median(long[] sorted) {
      int n = sorted.length;
      return n % 2 == 1 ? sorted[n / 2] : (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0;
    }

    Map<String, Object> measure(Filter f, int[] truth, int depth, int k, int ef) throws Exception {
      int qn = queries.length;
      int np = partCount();
      long[] visited = new long[np];
      double sum = 0;
      double worst = 1;
      double visitedSum = 0;
      double[] partSums = new double[3];
      for (int q = 0; q < qn; q++) {
        double r = recall(search(q, k, ef, f, visited), truth, depth, q, k);
        sum += r;
        worst = Math.min(worst, r);
        long[] sorted = visited.clone();
        Arrays.sort(sorted);
        visitedSum += Arrays.stream(sorted).sum();
        partSums[0] += sorted[0];
        partSums[1] += median(sorted);
        partSums[2] += sorted[np - 1];
      }
      List<Double> lat = new ArrayList<>();
      long done = 0;
      long t0 = System.nanoTime();
      int minQueries = Math.min(qn, 200);
      while (done < minQueries || System.nanoTime() - t0 < minTimeNs) {
        int q = (int) (done % qn);
        long a = System.nanoTime();
        search(q, k, ef, f, visited);
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
          long[] scratch = new long[np];
          while (System.nanoTime() - start < minTimeNs) {
            int q = (int) (next.getAndIncrement() % qn);
            search(q, k, ef, f, scratch);
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
      // Vectors compared per query, as Lucene counts its visits.
      p.put("visited_mean", visitedSum / qn);
      if (np > 1) {
        Map<String, Object> perPart = new LinkedHashMap<>();
        perPart.put("parts", np);
        perPart.put("visited", Map.of("min_mean", partSums[0] / qn, "median_mean", partSums[1] / qn,
            "max_mean", partSums[2] / qn));
        p.put("per_part", perPart);
      }
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
