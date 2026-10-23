package dev.portfolio.pulsestream.wire;

import org.junit.jupiter.api.Test;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class RespTest {
    private ByteArrayInputStream input(String s) { return new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)); }
    @Test void binaryPayloadRoundTrip() throws Exception {
        byte[] binary = {0, '\r', '\n', (byte) 255};
        var out = new ByteArrayOutputStream(); Resp.write(out, List.of("SET", "key", binary));
        var parsed = Resp.readCommand(new ByteArrayInputStream(out.toByteArray()));
        assertArrayEquals(binary, parsed.get(2));
    }
    @Test void cleanEofIsDifferentFromTruncation() throws Exception {
        assertNull(Resp.readCommand(input("")));
        assertThrows(IOException.class, () -> Resp.readCommand(input("*1\r\n$4\r\nabc")));
    }
    @Test void malformedAndOversizedFramesAreRejected() {
        for (String frame : List.of("*0\r\n", "*-1\r\n", "*17\r\n", "*1\r\n$1048577\r\n", "*1\r\n$-1\r\n", "*1\r\n$1\r\naXX", "*9999999999\r\n"))
            assertThrows(IOException.class, () -> Resp.readCommand(input(frame)), frame);
    }
    @Test void supportsPipelinedCommands() throws Exception {
        var in = input("*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nINFO\r\n");
        assertEquals("PING", Resp.text(Resp.readCommand(in).getFirst()));
        assertEquals("INFO", Resp.text(Resp.readCommand(in).getFirst())); assertNull(Resp.readCommand(in));
    }
    @Test void errorsCannotInjectResponses() throws Exception {
        var out = new ByteArrayOutputStream(); Resp.write(out, new Resp.Error("a\r\nb"));
        assertEquals("-ERR a  b\r\n", out.toString(StandardCharsets.UTF_8));
    }
}
