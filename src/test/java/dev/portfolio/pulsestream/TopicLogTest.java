package dev.portfolio.pulsestream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import static java.nio.file.StandardOpenOption.*;
import static org.junit.jupiter.api.Assertions.*;

class TopicLogTest {
    @TempDir Path root;
    private List<Path> segments() throws IOException { try (var files = Files.list(root)) { return files.filter(p -> p.toString().endsWith(".log")).sorted().toList(); } }
    @Test void restartPreservesOffsetsPayloadsAndSegmentRotation() throws Exception {
        try (var log = new TopicLog(root, 64, 1024)) {
            for (int i = 0; i < 7; i++) assertEquals(i, log.append(new byte[]{(byte) i}));
        }
        assertEquals(3, segments().size());
        try (var log = new TopicLog(root, 64, 1024)) {
            assertEquals(7, log.size()); var records = log.read(0, 16);
            for (int i = 0; i < 7; i++) { assertEquals(i, records.get(i).offset()); assertArrayEquals(new byte[]{(byte) i}, records.get(i).body()); }
            assertEquals(7, log.append(new byte[]{7})); assertTrue(log.read(8, 1).isEmpty());
        }
    }
    @Test void tornHeaderInActiveSegmentIsTruncated() throws Exception {
        try (var log = new TopicLog(root, 128, 1024)) { log.append(new byte[]{1, 2}); }
        Files.write(segments().getLast(), new byte[]{0, 1, 2}, APPEND);
        try (var log = new TopicLog(root, 128, 1024)) { assertEquals(1, log.size()); assertEquals(22, log.bytes()); assertEquals(1, log.append(new byte[]{3})); }
    }
    @Test void tornBodyInActiveSegmentIsTruncated() throws Exception {
        try (var log = new TopicLog(root, 128, 1024)) { log.append(new byte[]{1}); log.append(new byte[]{2, 3, 4}); }
        try (var channel = FileChannel.open(segments().getLast(), WRITE)) { channel.truncate(channel.size() - 1); }
        try (var log = new TopicLog(root, 128, 1024)) { assertEquals(1, log.size()); assertEquals(21, log.bytes()); }
    }
    @Test void crcCorruptionFailsClosedWithoutTruncatingData() throws Exception {
        try (var log = new TopicLog(root, 128, 1024)) { log.append(new byte[]{1, 2}); }
        Path segment = segments().getFirst();
        try (var channel = FileChannel.open(segment, WRITE)) { channel.write(ByteBuffer.wrap(new byte[]{99}), TopicLog.HEADER); }
        assertThrows(IOException.class, () -> new TopicLog(root, 128, 1024)); assertEquals(22, Files.size(segment));
    }
    @Test void sealedSegmentTruncationIsNotSilentlyRepaired() throws Exception {
        try (var log = new TopicLog(root, 32, 1024)) { log.append(new byte[]{1, 2}); log.append(new byte[]{3}); }
        try (var channel = FileChannel.open(segments().getFirst(), WRITE)) { channel.truncate(21); }
        assertThrows(IOException.class, () -> new TopicLog(root, 32, 1024));
    }
    @Test void deletedSegmentIsDetectedByOffsetGap() throws Exception {
        try (var log = new TopicLog(root, 32, 1024)) { log.append(new byte[]{1}); log.append(new byte[]{2}); }
        Files.delete(segments().getFirst()); assertThrows(IOException.class, () -> new TopicLog(root, 32, 1024));
    }
    @Test void quotasAndReadBoundsAreEnforced() throws Exception {
        try (var log = new TopicLog(root, 32, 42)) {
            log.append(new byte[]{1}); log.append(new byte[]{2});
            assertThrows(IllegalStateException.class, () -> log.append(new byte[]{3}));
            assertThrows(IllegalArgumentException.class, () -> log.read(-1, 1));
            assertThrows(IllegalArgumentException.class, () -> log.read(3, 1));
            assertThrows(IllegalArgumentException.class, () -> log.read(0, 17));
            assertEquals(2, log.size());
        }
    }
    @Test void concurrentProducersHaveUniqueContiguousOffsets() throws Exception {
        try (var log = new TopicLog(root, 1024, 1024 * 32); var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<Long>> writes = new ArrayList<>();
            for (int i = 0; i < 100; i++) writes.add(pool.submit(() -> log.append(new byte[]{1})));
            Set<Long> offsets = new HashSet<>(); for (var f : writes) offsets.add(f.get(20, TimeUnit.SECONDS));
            assertEquals(100, offsets.size()); assertTrue(offsets.contains(0L)); assertTrue(offsets.contains(99L));
        }
        try (var recovered = new TopicLog(root, 1024, 1024 * 32)) { assertEquals(100, recovered.size()); }
    }
    @Test void emptyMessagesAndClosedLogAreHandled() throws Exception {
        var log = new TopicLog(root, 128, 1024); log.append(new byte[0]);
        assertArrayEquals(new byte[0], log.read(0, 1).getFirst().body()); log.close(); log.close();
        assertThrows(IllegalStateException.class, () -> log.append(new byte[0]));
    }
    @Test void readsDetectCorruptionIntroducedAfterStartup() throws Exception {
        try (var log = new TopicLog(root, 128, 1024)) {
            log.append(new byte[]{1});
            try (var channel = FileChannel.open(segments().getFirst(), WRITE)) { channel.write(ByteBuffer.wrap(new byte[]{2}), TopicLog.HEADER); }
            assertThrows(IOException.class, () -> log.read(0, 1));
        }
    }
}
