package ai.pipestream.search.mobile;

/**
 * Byte-oriented Android entry point for the embedded Protomolt Search engine.
 *
 * <p>Every byte array is an encoded protobuf message. Every result is an
 * {@code ai.protomolt.search.mobile.v1.MobileResponse}. Search behavior and
 * lifecycle state live in the packaged Rust library; this class contains no
 * ranking, schema, persistence, or networking implementation.</p>
 */
public final class ProtomoltSearch {
    static {
        System.loadLibrary("protomolt_search_embedded");
    }

    private ProtomoltSearch() {}

    /** Accepts MobileOpenRequest and creates or opens a private local cluster. */
    public static native byte[] nativeOpen(byte[] request, boolean create);

    /** Accepts MobileIngestMappedBatch. */
    public static native byte[] nativeIngestMapped(long handle, byte[] request);

    /** AcceptDocumentRequest -> DocumentWriteReceipt. Call off the UI thread. */
    public static native byte[] nativeAcceptDocument(long handle, byte[] request);

    /** DescribeSchemaRequest -> DescribeSchemaResponse. Call off the UI thread. */
    public static native byte[] nativeDescribeSchema(long handle, byte[] request);

    /** PlanIndexRequest -> PlanIndexResponse. Call off the UI thread. */
    public static native byte[] nativePlanIndex(long handle, byte[] request);

    /** Reads original source history locally. Call off the UI thread. */
    public static native byte[] nativeReadAcceptedDocuments(long handle, byte[] request);

    /** Accepts the public QueryRequest. */
    public static native byte[] nativeQuery(long handle, byte[] request);

    /** Accepts QueryStreamRequest and returns MobileQueryStreamOpenResponse. */
    public static native byte[] nativeQueryStreamOpen(long handle, byte[] request);

    /** Returns the next MobileQueryStreamNextResponse without callbacks. */
    public static native byte[] nativeQueryStreamNext(long streamHandle);

    /** Cancels and closes a query stream. */
    public static native byte[] nativeQueryStreamClose(long streamHandle);

    /** Flushes every shard and returns MobileFlushResponse. */
    public static native byte[] nativeFlush(long handle);

    /** Closes a runtime handle and all of its open streams. */
    public static native byte[] nativeClose(long handle);
}
