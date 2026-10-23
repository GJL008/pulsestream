package dev.portfolio.pulsestream;

import dev.portfolio.pulsestream.wire.TcpServer;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;

public final class Main {
    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 9092;
        Broker broker = new Broker(Path.of(args.length > 1 ? args[1] : "data/pulsestream"));
        TcpServer server;
        try { server = new TcpServer("127.0.0.1", port, 256, broker::handle); }
        catch (Exception e) { broker.close(); throw e; }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { server.close(); } catch (Exception e) { System.err.println(e); }
            try { broker.close(); } catch (Exception e) { System.err.println(e); }
        }));
        server.start(); System.out.println("PulseStream listening on 127.0.0.1:" + server.port());
        new CountDownLatch(1).await();
    }
}
