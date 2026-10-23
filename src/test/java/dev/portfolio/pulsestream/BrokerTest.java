package dev.portfolio.pulsestream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class BrokerTest {
    @TempDir Path root;
    private List<byte[]> cmd(String... args) { return Arrays.stream(args).map(s -> s.getBytes(StandardCharsets.UTF_8)).toList(); }
    @Test void processLockIsExclusiveAndReleased() throws Exception {
        try (var broker = new Broker(root)) { assertThrows(IOException.class, () -> new Broker(root)); }
        try (var broker = new Broker(root)) { assertEquals("PONG", broker.handle(cmd("PING"))); }
    }
    @Test void topicsAreCaseInsensitiveAndConsumeReturnsNextOffset() throws Exception {
        try (var broker = new Broker(root)) {
            assertEquals(0L, broker.handle(cmd("PRODUCE", "Orders", "one")));
            assertEquals(1L, broker.handle(cmd("PRODUCE", "orders", "two")));
            var batch = (List<?>) broker.handle(cmd("CONSUME", "ORDERS", "0", "1"));
            assertEquals(1L, batch.getFirst()); assertEquals(1, ((List<?>) batch.get(1)).size());
            assertThrows(IllegalArgumentException.class, () -> broker.handle(cmd("PRODUCE", "../escape", "bad")));
            assertThrows(IllegalArgumentException.class, () -> broker.handle(cmd("CONSUME", "missing", "0")));
        }
        try (var broker = new Broker(root)) { assertTrue(broker.stats().contains("records=2")); }
    }
    @Test void topicCountIsBounded() throws Exception {
        try (var broker = new Broker(root)) {
            for (int i = 0; i < 16; i++) broker.handle(cmd("PRODUCE", "topic" + i, "x"));
            assertThrows(IllegalStateException.class, () -> broker.handle(cmd("PRODUCE", "overflow", "x")));
            assertEquals(1L, broker.handle(cmd("PRODUCE", "topic0", "still-writable")));
        }
    }
    @Test void invalidCatalogFailsStartupAndReleasesLock() throws Exception {
        Path invalid = Files.createDirectory(root.resolve("Uppercase"));
        assertThrows(IOException.class, () -> new Broker(root));
        Files.delete(invalid);
        try (var broker = new Broker(root)) { assertEquals("PONG", broker.handle(cmd("PING"))); }
    }
}
