package ai.pipestream.search.mobile.devicetest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import android.Manifest;
import android.content.Context;
import android.content.pm.PackageManager;
import android.os.Bundle;
import android.os.Debug;
import android.os.SystemClock;
import android.system.Os;

import androidx.test.core.app.ApplicationProvider;
import androidx.test.ext.junit.runners.AndroidJUnit4;
import androidx.test.filters.LargeTest;
import androidx.test.filters.RequiresDevice;
import androidx.test.platform.app.InstrumentationRegistry;

import ai.pipestream.search.mobile.ProtomoltSearch;
import ai.protomolt.search.mobile.v1.Mobile;
import ai.protomolt.search.v1.Search;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.WireFormat;

import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.Arrays;

@RunWith(AndroidJUnit4.class)
public final class ProtomoltSearchDeviceTest {
    private static final String FINGERPRINT =
            "e08ddb9a6de98aecd3861a430ca26774cbb990511f8c1445f98ee12f85f13d4a";
    private static final long FIXTURE_DISK_BUDGET = 64L * 1024L * 1024L;

    @Test
    public void persistenceBackgroundDiskAndNoEgress() throws Exception {
        Context context = ApplicationProvider.getApplicationContext();
        assertEquals(
                PackageManager.PERMISSION_DENIED,
                context.getPackageManager().checkPermission(
                        Manifest.permission.INTERNET, context.getPackageName()));

        File root = new File(context.getNoBackupFilesDir(), "protomolt-device-" + System.nanoTime());
        assertTrue(root.mkdirs());
        File index = new File(root, "private.tv");
        byte[] openRequest = openRequest(index);
        int socketsBefore = socketDescriptors();
        long handle = open(openRequest, true);
        try {
            ingest(handle);
            flush(handle);

            Search.QueryResponse query = query(handle);
            assertEquals(1, query.getHitsCount());
            stream(handle, query);

            long bytes = treeBytes(root);
            assertTrue("fixture wrote no durable bytes", bytes > 0);
            assertTrue("fixture exceeded disk budget: " + bytes, bytes < FIXTURE_DISK_BUDGET);

            // This is the SDK lifecycle required when the host backgrounds:
            // flush, release native tasks, then reopen the same generation.
            close(handle);
            handle = open(openRequest, false);
            assertEquals(1, query(handle).getHitsCount());
            close(handle);
            handle = 0;

            Mobile.MobileResponse refusal = Mobile.MobileResponse.parseFrom(
                    ProtomoltSearch.nativeOpen(openRequest, true));
            assertEquals(Mobile.MobileResponse.OutcomeCase.ERROR, refusal.getOutcomeCase());
            assertEquals(
                    Mobile.MobileErrorCode.MOBILE_ERROR_CODE_ALREADY_EXISTS,
                    refusal.getError().getCode());
        } finally {
            if (handle != 0) {
                close(handle);
            }
            deleteTree(root);
        }
        assertEquals("native SDK opened a socket", socketsBefore, socketDescriptors());
    }

    @Test
    @LargeTest
    @RequiresDevice
    public void queryPowerProbeReportsCpuAndWallTime() throws Exception {
        Context context = ApplicationProvider.getApplicationContext();
        File root = new File(context.getNoBackupFilesDir(), "protomolt-power-" + System.nanoTime());
        assertTrue(root.mkdirs());
        long handle = open(openRequest(new File(root, "private.tv")), true);
        try {
            ingest(handle);
            flush(handle);
            byte[] request = lexicalQuery().toByteArray();
            long cpuStart = Debug.threadCpuTimeNanos();
            long wallStart = SystemClock.elapsedRealtimeNanos();
            for (int i = 0; i < 100; i++) {
                payload(ProtomoltSearch.nativeQuery(handle, request));
            }
            long wallNanos = SystemClock.elapsedRealtimeNanos() - wallStart;
            long cpuNanos = Debug.threadCpuTimeNanos() - cpuStart;
            Bundle metrics = new Bundle();
            metrics.putLong("protomolt_query_100_wall_nanos", wallNanos);
            metrics.putLong("protomolt_query_100_caller_cpu_nanos", cpuNanos);
            metrics.putLong("protomolt_index_bytes", treeBytes(root));
            InstrumentationRegistry.getInstrumentation().sendStatus(2, metrics);
            assertTrue(wallNanos > 0);
            assertTrue(cpuNanos >= 0);
        } finally {
            close(handle);
            deleteTree(root);
        }
    }

    private static byte[] openRequest(File index) {
        Mobile.MobileShardConfig shard = Mobile.MobileShardConfig.newBuilder()
                .setIndexPath(index.getAbsolutePath())
                .addFacetFields("id")
                .build();
        return Mobile.MobileOpenRequest.newBuilder().addShards(shard).build().toByteArray();
    }

    private static long open(byte[] request, boolean create) throws Exception {
        Mobile.MobileOpenResponse opened = Mobile.MobileOpenResponse.parseFrom(
                payload(ProtomoltSearch.nativeOpen(request, create)));
        assertTrue(opened.getNoEgress());
        assertEquals(1, opened.getShardCount());
        return opened.getHandle();
    }

    private static void ingest(long handle) throws Exception {
        Search.MappedBind bind = Search.MappedBind.newBuilder()
                .setDescriptorSet(ByteString.copyFrom(descriptorSet()))
                .setMessageType("private.v1.Record")
                .setExpectedFingerprint(FINGERPRINT)
                .setBodyPath("body")
                .setAnalysis(bodySpec())
                .build();
        Mobile.MobileIngestMappedBatch batch = Mobile.MobileIngestMappedBatch.newBuilder()
                .setShard(0)
                .addRequests(Search.IngestMappedRequest.newBuilder().setBind(bind))
                .addRequests(Search.IngestMappedRequest.newBuilder()
                        .setDocument(ByteString.copyFrom(document())))
                .build();
        Search.IngestMappedResponse result = Search.IngestMappedResponse.parseFrom(
                payload(ProtomoltSearch.nativeIngestMapped(handle, batch.toByteArray())));
        assertEquals(1, result.getAdded());
    }

    private static void flush(long handle) throws Exception {
        Mobile.MobileFlushResponse response = Mobile.MobileFlushResponse.parseFrom(
                payload(ProtomoltSearch.nativeFlush(handle)));
        assertEquals(1, response.getShardsCount());
        assertTrue(response.getShards(0).getWritten());
    }

    private static Search.QueryResponse query(long handle) throws Exception {
        return Search.QueryResponse.parseFrom(
                payload(ProtomoltSearch.nativeQuery(handle, lexicalQuery().toByteArray())));
    }

    private static void stream(long handle, Search.QueryResponse unary) throws Exception {
        Search.QueryStreamRequest request = Search.QueryStreamRequest.newBuilder()
                .setQuery(lexicalQuery())
                .build();
        Mobile.MobileQueryStreamOpenResponse opened =
                Mobile.MobileQueryStreamOpenResponse.parseFrom(payload(
                        ProtomoltSearch.nativeQueryStreamOpen(handle, request.toByteArray())));
        int completions = 0;
        while (true) {
            Mobile.MobileQueryStreamNextResponse next =
                    Mobile.MobileQueryStreamNextResponse.parseFrom(payload(
                            ProtomoltSearch.nativeQueryStreamNext(opened.getStreamHandle())));
            if (next.getEnd()) {
                break;
            }
            assertTrue(next.hasResponse());
            if (next.getResponse().getPayloadCase()
                    == Search.QueryStreamResponse.PayloadCase.COMPLETION) {
                completions++;
                assertTrue(next.getResponse().getCompletion().getCompleted());
                assertEquals(unary, next.getResponse().getCompletion().getResponse());
            }
        }
        assertEquals(1, completions);
    }

    private static void close(long handle) throws Exception {
        Mobile.MobileCloseResponse response = Mobile.MobileCloseResponse.parseFrom(
                payload(ProtomoltSearch.nativeClose(handle)));
        assertTrue(response.getClosed());
    }

    private static byte[] payload(byte[] encoded) throws Exception {
        Mobile.MobileResponse response = Mobile.MobileResponse.parseFrom(encoded);
        assertEquals(
                response.hasError() ? response.getError().getMessage() : "",
                Mobile.MobileResponse.OutcomeCase.PAYLOAD,
                response.getOutcomeCase());
        return response.getPayload().toByteArray();
    }

    private static Search.QueryRequest lexicalQuery() {
        Search.LexicalQuery lexical = Search.LexicalQuery.newBuilder()
                .setText("zebra")
                .setAnalysis(bodySpec())
                .build();
        Search.SearchQuery search = Search.SearchQuery.newBuilder()
                .setId("lexical")
                .setLexical(lexical)
                .build();
        return Search.QueryRequest.newBuilder()
                .setRequestId("android-device")
                .setK(10)
                .setSelectionK(10)
                .setSelection(Search.SelectionQuery.newBuilder().setSearch(search))
                .build();
    }

    private static Search.AnalysisSpec bodySpec() {
        return Search.AnalysisSpec.newBuilder()
                .setTokenizer(1)
                .setStemmer(2)
                .setTermVectorMode(1)
                .setTermVectorSource(3)
                .addAllCharFilters(Arrays.asList(1, 2, 15, 6))
                .build();
    }

    private static byte[] descriptorSet() {
        DescriptorProtos.DescriptorProto record = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Record")
                .addField(field("id", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING,
                        DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .addField(field("body", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING,
                        DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .addField(field("embedding", 3, DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT,
                        DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED))
                .build();
        DescriptorProtos.FileDescriptorProto file =
                DescriptorProtos.FileDescriptorProto.newBuilder()
                        .setName("private.proto")
                        .setPackage("private.v1")
                        .addMessageType(record)
                        .build();
        return DescriptorProtos.FileDescriptorSet.newBuilder().addFile(file).build().toByteArray();
    }

    private static DescriptorProtos.FieldDescriptorProto field(
            String name,
            int number,
            DescriptorProtos.FieldDescriptorProto.Type type,
            DescriptorProtos.FieldDescriptorProto.Label label) {
        return DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName(name)
                .setNumber(number)
                .setType(type)
                .setLabel(label)
                .build();
    }

    private static byte[] document() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        CodedOutputStream coded = CodedOutputStream.newInstance(bytes);
        coded.writeString(1, "mobile-1");
        coded.writeString(2, "private mobile zebra");
        coded.writeTag(3, WireFormat.WIRETYPE_LENGTH_DELIMITED);
        coded.writeUInt32NoTag(32 * Float.BYTES);
        for (int i = 0; i < 32; i++) {
            coded.writeFixed32NoTag(Float.floatToRawIntBits(0.125f));
        }
        coded.flush();
        return bytes.toByteArray();
    }

    private static int socketDescriptors() {
        File[] descriptors = new File("/proc/self/fd").listFiles();
        if (descriptors == null) {
            return 0;
        }
        int sockets = 0;
        for (File descriptor : descriptors) {
            try {
                if (Os.readlink(descriptor.getAbsolutePath()).startsWith("socket:[")) {
                    sockets++;
                }
            } catch (Exception ignored) {
                // A descriptor may close between listFiles and readlink.
            }
        }
        return sockets;
    }

    private static long treeBytes(File file) {
        if (file.isFile()) {
            return file.length();
        }
        File[] children = file.listFiles();
        if (children == null) {
            return 0;
        }
        long total = 0;
        for (File child : children) {
            total += treeBytes(child);
        }
        return total;
    }

    private static void deleteTree(File file) {
        File[] children = file.listFiles();
        if (children != null) {
            for (File child : children) {
                deleteTree(child);
            }
        }
        assertTrue("could not delete " + file, file.delete() || !file.exists());
    }
}
