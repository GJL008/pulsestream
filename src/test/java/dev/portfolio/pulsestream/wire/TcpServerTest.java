package dev.portfolio.pulsestream.wire;

import org.junit.jupiter.api.Test;
import java.net.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class TcpServerTest {
    @Test void applicationErrorDoesNotKillSessionAndMalformedFrameClosesIt() throws Exception {
        try (var server = new TcpServer("127.0.0.1", 0, 8, args -> {
            if (Resp.text(args.getFirst()).equals("BAD")) throw new IllegalArgumentException("bad command");
            return "PONG";
        })) {
            server.start();
            try (Socket socket = new Socket("127.0.0.1", server.port())) {
                socket.setSoTimeout(3000);
                var out = socket.getOutputStream(); var in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                Resp.write(out, List.of("BAD")); out.flush(); assertEquals("-ERR bad command", in.readLine());
                Resp.write(out, List.of("PING")); out.flush(); assertEquals("$4", in.readLine()); assertEquals("PONG", in.readLine());
                Resp.write(out, List.of("METRICS")); out.flush(); assertTrue(in.readLine().startsWith("$"));
                String metrics = in.readLine(); assertTrue(metrics.contains("commands=3")); assertTrue(metrics.contains("errors=1"));
                out.write("*999\r\n".getBytes(StandardCharsets.UTF_8)); out.flush();
                assertTrue(in.readLine().startsWith("-ERR protocol:")); assertNull(in.readLine());
            }
        }
    }
    @Test void closeUnblocksIdleClient() throws Exception {
        var server = new TcpServer("127.0.0.1", 0, 8, args -> "OK"); server.start();
        try (Socket socket = new Socket("127.0.0.1", server.port())) {
            socket.setSoTimeout(3000);
            Resp.write(socket.getOutputStream(), List.of("PING")); socket.getOutputStream().flush();
            assertEquals('$', socket.getInputStream().read());
            server.close(); socket.getInputStream().readAllBytes();
        } finally { server.close(); }
    }
}
