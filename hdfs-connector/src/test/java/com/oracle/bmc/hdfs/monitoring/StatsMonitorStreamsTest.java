package com.oracle.bmc.hdfs.monitoring;

import com.oracle.bmc.hdfs.store.BmcPropertyAccessor;
import org.apache.hadoop.fs.FSInputStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

@RunWith(PowerMockRunner.class)
public class StatsMonitorStreamsTest {

    // Simple in-memory FSInputStream used by the input-stream test
    static class DummyFSIS extends FSInputStream {
        private final byte[] data;
        private int pos = 0;
        DummyFSIS(byte[] data) { this.data = data; }
        @Override public void seek(long l) { pos = (int) l; }
        @Override public long getPos() { return pos; }
        @Override public boolean seekToNewSource(long l) { pos = (int) l; return true; }
        @Override public int read() { return (pos < data.length) ? (data[pos++] & 0xFF) : -1; }
        @Override public int read(byte[] b, int off, int len) {
            if (pos >= data.length) return -1;
            int n = Math.min(len, data.length - pos);
            System.arraycopy(data, pos, b, off, n);
            pos += n;
            return n;
        }
        @Override public void close() {}
        @Override public int available() { return Math.max(0, data.length - pos); }
    }

    @Test
    public void wrapsFsInputStreamAndEmitsMetrics() throws IOException, InterruptedException {
        byte[] payload = "hello world".getBytes();
        FSInputStream src = new DummyFSIS(payload);

        SpyPlugin plugin = new SpyPlugin(mock(BmcPropertyAccessor.class), "bucketA", "grp", "ns");
        OCIMonitorPluginHandler handler = new OCIMonitorPluginHandler();
        handler.setEnabled(true);
        handler.setBucketName("bucketA");
        handler.setListOfPlugins(Collections.singletonList(plugin));
        handler.setMaxBacklogBeforeDrop(100);
        handler.init();

        RetryMetricsCollector collector = new RetryMetricsCollector("READ", 60L, 10L);
        StatsMonitorInputStream wrapped = new StatsMonitorInputStream(src, handler, collector);

        assertEquals(payload.length, wrapped.available());
        byte[] buf = new byte[5];
        int n1 = wrapped.read(buf);
        assertEquals(5, n1);
        assertEquals('h', buf[0]);

        int n2 = wrapped.read(buf, 0, buf.length);
        assertTrue(n2 > 0);

        wrapped.seek(0);
        assertEquals(0, wrapped.getPos());
        assertTrue(wrapped.seekToNewSource(1));

        wrapped.close(); // should emit and not throw

        // async fan-out: wait briefly for at least one metric to be accepted
        long deadline = System.currentTimeMillis() + 2000;
        while (plugin.accepted.size() == 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertTrue("expected at least one emitted metric", plugin.accepted.size() > 0);
    }

    @Test
    public void writesFlushesClosesAndEmits() throws IOException, InterruptedException {
        ByteArrayOutputStream sink = new ByteArrayOutputStream();

        SpyPlugin plugin = new SpyPlugin(mock(BmcPropertyAccessor.class), "bucketA", "grp", "ns");
        OCIMonitorPluginHandler handler = new OCIMonitorPluginHandler();
        handler.setEnabled(true);
        handler.setBucketName("bucketA");
        handler.setListOfPlugins(Collections.singletonList(plugin));
        handler.setMaxBacklogBeforeDrop(100);
        handler.init();

        RetryMetricsCollector collector = new RetryMetricsCollector("WRITE", 60L, 10L);
        StatsMonitorOutputStream out = new StatsMonitorOutputStream(sink, handler, collector);

        out.write('A');
        out.write("BCDEF".getBytes());
        out.write("0123456789".getBytes(), 2, 4); // "2345"
        out.flush();
        out.close(); // idempotent

        String s = new String(sink.toByteArray());
        assertEquals("ABCDEF2345", s);

        // async fan-out: wait briefly for at least one metric to be accepted
        long deadline = System.currentTimeMillis() + 2000;
        while (plugin.accepted.size() == 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertTrue("expected at least one emitted metric", plugin.accepted.size() > 0);
    }
}
