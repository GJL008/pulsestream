package dev.portfolio.pulsestream;

import java.io.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import dev.portfolio.pulsestream.wire.Resp;
import static java.nio.file.StandardOpenOption.*;

public final class Broker implements AutoCloseable {
    private static final int MAX_TOPICS = 16;
    private final Path root;
    private final FileChannel lockChannel;
    private final FileLock processLock;
    private final Map<String, TopicLog> topics = new TreeMap<>();
    private final ReentrantLock catalog = new ReentrantLock();
    private boolean closed;

    public Broker(Path root) throws IOException {
        this.root = root.toAbsolutePath().normalize(); Files.createDirectories(this.root);
        lockChannel = FileChannel.open(this.root.resolve(".lock"), CREATE, WRITE);
        FileLock acquired;
        try { acquired = lockChannel.tryLock(); }
        catch (IOException | OverlappingFileLockException e) { lockChannel.close(); throw new IOException("data directory already locked or inaccessible", e); }
        if (acquired == null) { lockChannel.close(); throw new IOException("data directory already locked"); }
        processLock = acquired;
        try (var paths = Files.list(this.root)) {
            for (Path path : paths.filter(Files::isDirectory).sorted().toList()) {
                String name = path.getFileName().toString();
                if (!name.equals(name.toLowerCase(Locale.ROOT)) || Files.isSymbolicLink(path))
                    throw new IOException("topic directories must be lowercase and must not be symbolic links");
                topic(name, true);
            }
        } catch (IOException | RuntimeException e) { try { close(); } catch (IOException suppressed) { e.addSuppressed(suppressed); } throw e; }
    }
    private TopicLog topic(String name, boolean create) throws IOException {
        if (!name.matches("[a-zA-Z0-9_-]{1,64}")) throw new IllegalArgumentException("topic must match [a-zA-Z0-9_-]{1,64}");
        catalog.lock();
        try {
            if (closed) throw new IllegalStateException("broker closed");
            // Case-insensitive uniqueness makes data portable between Windows and Linux.
            String canonical = name.toLowerCase(Locale.ROOT);
            TopicLog log = topics.get(canonical);
            if (log == null && create) {
                if (topics.size() >= MAX_TOPICS) throw new IllegalStateException("topic limit exhausted");
                log = new TopicLog(root.resolve(canonical), 4L * 1024 * 1024, 64L * 1024 * 1024);
                topics.put(canonical, log);
            }
            if (log == null) throw new IllegalArgumentException("unknown topic");
            return log;
        } finally { catalog.unlock(); }
    }
    public Object handle(List<byte[]> args) throws IOException {
        String verb = Resp.text(args.getFirst()).toUpperCase(Locale.ROOT);
        return switch (verb) {
            case "PING" -> { Resp.arity(args, 1); yield "PONG"; }
            case "PRODUCE" -> { Resp.arity(args, 3); yield topic(Resp.text(args.get(1)), true).append(args.get(2)); }
            case "CONSUME" -> {
                if (args.size() != 3 && args.size() != 4) throw new IllegalArgumentException("CONSUME topic offset [limit]");
                long offset = Long.parseLong(Resp.text(args.get(2)));
                int limit = args.size() == 4 ? Integer.parseInt(Resp.text(args.get(3))) : 16;
                List<TopicLog.Message> messages = topic(Resp.text(args.get(1)), false).read(offset, limit);
                List<Object> records = new ArrayList<>();
                for (var message : messages) records.add(List.of(message.offset(), message.timestamp(), message.body()));
                yield List.of(offset + messages.size(), records);
            }
            case "INFO" -> { Resp.arity(args, 1); yield stats(); }
            default -> throw new IllegalArgumentException("unknown command: " + verb);
        };
    }
    public String stats() {
        catalog.lock();
        try {
            long records = 0, bytes = 0;
            for (TopicLog topic : topics.values()) { records += topic.size(); bytes += topic.bytes(); }
            return "topics=" + topics.size() + " records=" + records + " log_bytes=" + bytes + " durability=force-per-record";
        } finally { catalog.unlock(); }
    }
    @Override public void close() throws IOException {
        catalog.lock();
        try {
            if (closed) return; closed = true;
            IOException failure = null;
            for (TopicLog log : topics.values()) try { log.close(); } catch (IOException e) { failure = e; }
            try { processLock.release(); } finally { lockChannel.close(); }
            if (failure != null) throw failure;
        } finally { catalog.unlock(); }
    }
}
