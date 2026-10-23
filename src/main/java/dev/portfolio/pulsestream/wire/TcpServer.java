package dev.portfolio.pulsestream.wire;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** Loopback by default; one virtual thread per admitted connection. */
public final class TcpServer implements AutoCloseable {
    @FunctionalInterface public interface Handler { Object handle(List<byte[]> command) throws Exception; }
    private final ServerSocket listener;
    private final Handler handler;
    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
    private final Set<Socket> clients = ConcurrentHashMap.newKeySet();
    private final Semaphore permits;
    private final AtomicBoolean closing = new AtomicBoolean();
    private final LongAdder commands = new LongAdder(), errors = new LongAdder(), rejected = new LongAdder();
    private Thread acceptor;

    public TcpServer(String host, int port, int maxConnections, Handler handler) throws IOException {
        if (maxConnections < 1) throw new IllegalArgumentException("maxConnections must be positive");
        this.handler = handler; this.permits = new Semaphore(maxConnections);
        listener = new ServerSocket(); listener.setReuseAddress(true);
        listener.bind(new InetSocketAddress(host, port), 128);
    }
    public int port() { return listener.getLocalPort(); }
    public void start() { acceptor = Thread.ofPlatform().name("accept-" + port()).start(this::accept); }
    private void accept() {
        while (!closing.get()) {
            try {
                Socket socket = listener.accept();
                socket.setTcpNoDelay(true); socket.setSoTimeout(30_000);
                if (!permits.tryAcquire()) { rejected.increment(); socket.close(); continue; }
                clients.add(socket);
                try { workers.submit(() -> serve(socket)); }
                catch (RejectedExecutionException e) { clients.remove(socket); permits.release(); socket.close(); }
            } catch (IOException e) { if (!closing.get()) System.err.println("accept: " + e.getMessage()); }
        }
    }
    private void serve(Socket socket) {
        try (socket; var in = new BufferedInputStream(socket.getInputStream());
             var out = new BufferedOutputStream(socket.getOutputStream())) {
            while (!closing.get()) {
                List<byte[]> command;
                try { command = Resp.readCommand(in); }
                catch (IOException e) { errors.increment(); Resp.write(out, new Resp.Error("protocol: " + e.getMessage())); out.flush(); return; }
                if (command == null) return;
                commands.increment(); Object result;
                try {
                    if (Resp.text(command.getFirst()).equalsIgnoreCase("METRICS")) {
                        Resp.arity(command, 1); result = stats();
                    } else result = handler.handle(command);
                }
                catch (IllegalArgumentException | IllegalStateException e) { errors.increment(); result = new Resp.Error(e.getMessage()); }
                catch (Exception e) { errors.increment(); System.err.println("request failed: " + e); result = new Resp.Error("internal failure"); }
                Resp.write(out, result); out.flush();
            }
        } catch (IOException ignored) {
            // Client disconnects and idle timeouts are normal connection termination.
        } finally { clients.remove(socket); permits.release(); }
    }
    public String stats() { return "connections=" + clients.size() + " commands=" + commands.sum() + " errors=" + errors.sum() + " rejected=" + rejected.sum(); }
    @Override public void close() throws IOException {
        if (!closing.compareAndSet(false, true)) return;
        listener.close();
        for (Socket socket : clients) try { socket.close(); } catch (IOException ignored) {}
        workers.shutdown();
        try {
            if (acceptor != null) acceptor.join(5000);
            if (!workers.awaitTermination(10, TimeUnit.SECONDS)) workers.shutdownNow();
        } catch (InterruptedException e) { Thread.currentThread().interrupt(); workers.shutdownNow(); }
    }
}
