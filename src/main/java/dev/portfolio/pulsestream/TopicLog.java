package dev.portfolio.pulsestream;

import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32C;
import static java.nio.file.StandardOpenOption.*;

/** One ordered, durable partition. Acknowledgement follows force(true). */
public final class TopicLog implements AutoCloseable {
    public static final int HEADER = 20, MAX_MESSAGE = 64 * 1024, MAX_RECORDS = 100_000;
    private static final int MAGIC = 0x50554C31;
    public record Message(long offset, long timestamp, byte[] body) {}
    private record Pointer(Path path, long position, int length, long timestamp) {}
    private final Path directory;
    private final long segmentBytes, maxBytes;
    private final List<Pointer> index = new ArrayList<>();
    private final ReentrantLock lock = new ReentrantLock();
    private FileChannel active;
    private Path activePath;
    private long usedBytes;
    private boolean failed, closed;

    public TopicLog(Path directory, long segmentBytes, long maxBytes) throws IOException {
        if (segmentBytes < HEADER + 1 || maxBytes < segmentBytes) throw new IllegalArgumentException("invalid log limits");
        this.directory = directory; this.segmentBytes = segmentBytes; this.maxBytes = maxBytes;
        Files.createDirectories(directory);
        try { recover(); } catch (IOException | RuntimeException e) { if (active != null) active.close(); throw e; }
    }
    private void recover() throws IOException {
        List<Path> paths;
        try (var files = Files.list(directory)) {
            paths = files.filter(p -> p.getFileName().toString().matches("[0-9]{20}\\.log")).sorted().toList();
        }
        for (int n = 0; n < paths.size(); n++) {
            Path path = paths.get(n);
            long base;
            try { base = Long.parseLong(path.getFileName().toString().substring(0, 20)); }
            catch (NumberFormatException e) { throw new IOException("invalid segment name", e); }
            if (base != index.size()) throw new IOException("non-contiguous segment offsets: " + path);
            boolean last = n == paths.size() - 1;
            try (FileChannel channel = FileChannel.open(path, READ, WRITE)) {
                long size = channel.size(), position = 0;
                if (size > segmentBytes) throw new IOException("segment exceeds configured size");
                while (position < size) {
                    if (size - position < HEADER) { truncateTail(channel, position, last); break; }
                    ByteBuffer header = ByteBuffer.allocate(HEADER);
                    readFully(channel, header, position); header.flip();
                    int magic = header.getInt(), length = header.getInt();
                    long timestamp = header.getLong(); int checksum = header.getInt();
                    if (magic != MAGIC || length < 0 || length > MAX_MESSAGE || HEADER + (long) length > segmentBytes)
                        throw new IOException("invalid record header at " + path + ":" + position);
                    if (size - position - HEADER < length) { truncateTail(channel, position, last); break; }
                    ByteBuffer body = ByteBuffer.allocate(length);
                    readFully(channel, body, position + HEADER);
                    if (checksum(timestamp, body.array()) != checksum) throw new IOException("CRC mismatch at " + path + ":" + position);
                    if (index.size() >= MAX_RECORDS) throw new IOException("record count exceeds limit");
                    index.add(new Pointer(path, position, length, timestamp));
                    position += HEADER + length;
                }
                usedBytes += channel.size();
                if (usedBytes > maxBytes) throw new IOException("log exceeds configured quota");
            }
        }
        if (paths.isEmpty()) roll();
        else {
            activePath = paths.getLast(); active = FileChannel.open(activePath, READ, WRITE);
            active.position(active.size());
        }
    }
    private static void truncateTail(FileChannel channel, long position, boolean last) throws IOException {
        if (!last) throw new IOException("incomplete record in sealed segment");
        channel.truncate(position); channel.force(true);
    }
    private void roll() throws IOException {
        if (active != null) active.close();
        activePath = directory.resolve(String.format(Locale.ROOT, "%020d.log", index.size()));
        active = FileChannel.open(activePath, CREATE_NEW, READ, WRITE);
    }
    private static int checksum(long timestamp, byte[] body) {
        CRC32C crc = new CRC32C();
        crc.update(ByteBuffer.allocate(8).putLong(timestamp).array()); crc.update(body); return (int) crc.getValue();
    }
    public long append(byte[] body) throws IOException {
        if (body.length > MAX_MESSAGE) throw new IllegalArgumentException("message exceeds 65536 bytes");
        lock.lock();
        try {
            healthy();
            long bytes = HEADER + (long) body.length;
            if (bytes > segmentBytes) throw new IllegalArgumentException("message exceeds segment capacity");
            if (usedBytes + bytes > maxBytes || index.size() >= MAX_RECORDS) throw new IllegalStateException("topic quota exhausted");
            try {
                if (active.size() + bytes > segmentBytes) roll();
                long position = active.size(), offset = index.size(), timestamp = System.currentTimeMillis();
                ByteBuffer frame = ByteBuffer.allocate((int) bytes).putInt(MAGIC).putInt(body.length).putLong(timestamp).putInt(checksum(timestamp, body)).put(body);
                frame.flip(); while (frame.hasRemaining()) active.write(frame);
                active.force(true);
                index.add(new Pointer(activePath, position, body.length, timestamp)); usedBytes += bytes;
                return offset;
            } catch (IOException e) { failed = true; throw e; }
        } finally { lock.unlock(); }
    }
    public List<Message> read(long offset, int limit) throws IOException {
        if (offset < 0 || limit < 1 || limit > 16) throw new IllegalArgumentException("offset >= 0 and limit 1..16 required");
        lock.lock();
        try {
            healthy();
            if (offset > index.size()) throw new IllegalArgumentException("offset beyond log end");
            List<Message> messages = new ArrayList<>();
            int end = (int) Math.min(index.size(), offset + limit);
            for (int i = (int) offset; i < end; i++) {
                Pointer p = index.get(i);
                try (FileChannel channel = FileChannel.open(p.path, READ)) {
                    ByteBuffer body = ByteBuffer.allocate(p.length);
                    readFully(channel, body, p.position + HEADER);
                    // Detect changes to persisted records instead of returning silently corrupted data.
                    ByteBuffer header = ByteBuffer.allocate(HEADER); readFully(channel, header, p.position); header.flip();
                    if (header.getInt() != MAGIC || header.getInt() != p.length || header.getLong() != p.timestamp || header.getInt() != checksum(p.timestamp, body.array()))
                        throw new IOException("record integrity failure at offset " + i);
                    messages.add(new Message(i, p.timestamp, body.array()));
                }
            }
            return messages;
        } finally { lock.unlock(); }
    }
    private static void readFully(FileChannel channel, ByteBuffer target, long position) throws IOException {
        while (target.hasRemaining()) {
            int n = channel.read(target, position);
            if (n < 0) throw new EOFException("truncated record");
            position += n;
        }
    }
    public long size() { lock.lock(); try { return index.size(); } finally { lock.unlock(); } }
    public long bytes() { lock.lock(); try { return usedBytes; } finally { lock.unlock(); } }
    private void healthy() { if (closed || failed) throw new IllegalStateException("log unavailable; restart and recover before retrying"); }
    @Override public void close() throws IOException {
        lock.lock(); try { if (!closed) { closed = true; if (active != null) active.close(); } } finally { lock.unlock(); }
    }
}
