package dev.portfolio.pulsestream.wire;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/** A bounded RESP2 subset: requests are arrays of non-null bulk strings. */
public final class Resp {
    public static final int MAX_BULK = 1024 * 1024;
    private Resp() {}

    public static List<byte[]> readCommand(InputStream in) throws IOException {
        int first = in.read();
        if (first == -1) return null;
        if (first != '*') throw new IOException("expected RESP array");
        int count = number(in, 16);
        if (count < 1) throw new IOException("empty command");
        List<byte[]> values = new ArrayList<>(count);
        int total = 0;
        for (int i = 0; i < count; i++) {
            if (in.read() != '$') throw new IOException("expected bulk string");
            int size = number(in, MAX_BULK);
            total += size;
            if (total > MAX_BULK) throw new IOException("command too large");
            byte[] value = in.readNBytes(size);
            if (value.length != size || in.read() != '\r' || in.read() != '\n')
                throw new EOFException("truncated bulk string");
            values.add(value);
        }
        return values;
    }

    private static int number(InputStream in, int max) throws IOException {
        int n = 0, digits = 0;
        while (true) {
            int c = in.read();
            if (c == '\r') {
                if (digits == 0 || in.read() != '\n') throw new IOException("invalid length");
                return n;
            }
            if (c < '0' || c > '9' || ++digits > 9) throw new IOException("invalid length");
            n = n * 10 + c - '0';
            if (n > max) throw new IOException("length limit exceeded");
        }
    }

    public record Error(String message) {}
    public static void write(OutputStream out, Object value) throws IOException {
        if (value == null) ascii(out, "$-1\r\n");
        else if (value instanceof Error e) ascii(out, "-ERR " + e.message().replace('\r', ' ').replace('\n', ' ') + "\r\n");
        else if (value instanceof Number n) ascii(out, ":" + n.longValue() + "\r\n");
        else if (value instanceof List<?> list) {
            ascii(out, "*" + list.size() + "\r\n");
            for (Object item : list) write(out, item);
        } else {
            byte[] bytes = value instanceof byte[] b ? b : value.toString().getBytes(StandardCharsets.UTF_8);
            ascii(out, "$" + bytes.length + "\r\n"); out.write(bytes); ascii(out, "\r\n");
        }
    }
    private static void ascii(OutputStream out, String s) throws IOException { out.write(s.getBytes(StandardCharsets.UTF_8)); }
    public static String text(byte[] bytes) { return new String(bytes, StandardCharsets.UTF_8); }
    public static void arity(List<byte[]> args, int expected) {
        if (args.size() != expected) throw new IllegalArgumentException("wrong number of arguments");
    }
}
