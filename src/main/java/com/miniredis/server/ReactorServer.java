package com.miniredis.server;

import com.miniredis.command.CommandDispatcher;
import com.miniredis.protocol.RespDecoder;
import com.miniredis.protocol.RespEncoder;
import com.miniredis.protocol.RespValue;
import com.miniredis.store.ExpiryEngine;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * The single-threaded NIO reactor: one thread, one Selector, handling
 * every connected client's accept/read/write events plus periodic active
 * expiry, all without ever blocking on any individual client.
 *
 * This is the architectural core of the whole project -- everything else
 * (protocol parsing, commands, store) is logic that gets INVOKED from
 * inside this loop, but this loop is what makes the server actually able
 * to serve many concurrent clients on one thread with no locks anywhere.
 */
public final class ReactorServer {

    private static final int PORT = 6379;

    // How often (in milliseconds) to run an active-expiry cycle, even if
    // no client activity is happening. This is the select() timeout --
    // see runEventLoop() for exactly how it's used.
    private static final long EXPIRY_CYCLE_INTERVAL_MS = 100;

    private final CommandDispatcher dispatcher;
    private final ExpiryEngine expiryEngine;
    private final com.miniredis.persistence.AofWriter aofWriter;

    private Selector selector;
    private ServerSocketChannel serverChannel;

    public ReactorServer(CommandDispatcher dispatcher, ExpiryEngine expiryEngine,
                          com.miniredis.persistence.AofWriter aofWriter) {
        this.dispatcher = dispatcher;
        this.expiryEngine = expiryEngine;
        this.aofWriter = aofWriter;
    }

    /**
     * Sets up the listening socket and selector, then runs the event
     * loop forever (until the process is killed). This method does not
     * return under normal operation -- calling it IS "starting the
     * server."
     */
    public void start() throws IOException {
        selector = Selector.open();

        serverChannel = ServerSocketChannel.open();
        serverChannel.bind(new InetSocketAddress(PORT));

        // NON-BLOCKING MODE: this single line is what makes this a
        // reactor server rather than a traditional blocking server.
        // With this set, accept()/read()/write() calls on this channel
        // (and every SocketChannel it produces) NEVER block -- they
        // return immediately, either with real data/a connection, or
        // with "nothing right now" (0 bytes, or null for accept()).
        serverChannel.configureBlocking(false);

        // Register this channel with the selector, declaring we're
        // interested in OP_ACCEPT events -- "tell me when a new client
        // is trying to connect."
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        System.out.println("Mini Redis listening on port " + PORT);

        runEventLoop();
    }

    /**
     * The reactor loop itself. Structurally: wait for something to be
     * ready (or the expiry timeout to elapse), handle everything that's
     * ready, run an expiry cycle, repeat forever.
     */
    private void runEventLoop() throws IOException {
        while (true) {
            // Blocks THIS thread (only this one, there is no other) until
            // either a channel becomes ready OR EXPIRY_CYCLE_INTERVAL_MS
            // milliseconds pass, whichever happens first. This timeout is
            // exactly what lets active expiry run periodically without a
            // second thread -- we simply never wait longer than 100ms
            // before coming back around the loop to also run a cycle.
            selector.select(EXPIRY_CYCLE_INTERVAL_MS);

            // selectedKeys() returns the set of SelectionKeys whose
            // channels are ready for whatever they were registered for.
            Set<SelectionKey> readyKeys = selector.selectedKeys();
            Iterator<SelectionKey> iterator = readyKeys.iterator();

            while (iterator.hasNext()) {
                SelectionKey key = iterator.next();
                // CRITICAL: the selector does NOT automatically remove
                // keys from selectedKeys() once you've handled them --
                // you must remove each one yourself via the iterator.
                // Forgetting this is one of THE classic NIO bugs: keys
                // you already processed would still be sitting in the
                // set on the NEXT select() call too, causing the same
                // event to be handled repeatedly forever.
                iterator.remove();

                try {
                    if (!key.isValid()) {
                        continue; // channel was closed by an earlier key in this same batch
                    }
                    if (key.isAcceptable()) {
                        handleAccept(key);
                    } else if (key.isReadable()) {
                        handleRead(key);
                    } else if (key.isWritable()) {
                        handleWrite(key);
                    }
                } catch (IOException e) {
                    // A single client's socket erroring (connection reset,
                    // etc.) must NOT crash the whole reactor thread -- that
                    // would take down every other connected client too.
                    // Close just this one connection and move on.
                    closeConnection(key);
                }
            }

            // Runs every loop iteration, whether triggered by real client
            // activity or by the select() timeout expiring on an idle
            // server. This is the "cooperative scheduling" mentioned when
            // we built ExpiryEngine -- it only ever runs on this one
            // thread, interleaved between handling real client events.
            expiryEngine.runCycle();
        }
    }

    /**
     * OP_ACCEPT fired: a new client is trying to connect. Accept it,
     * configure its socket as non-blocking too, wrap it in a
     * ClientConnection, and register it for OP_READ -- "tell me when
     * this specific client sends data."
     */
    private void handleAccept(SelectionKey key) throws IOException {
        ServerSocketChannel server = (ServerSocketChannel) key.channel();
        SocketChannel clientChannel = server.accept();

        if (clientChannel == null) {
            // Can happen even after an OP_ACCEPT-ready signal, in rare
            // cases -- just means there's nothing to actually accept
            // right now. Safe to simply return and let the loop continue.
            return;
        }

        clientChannel.configureBlocking(false);
        ClientConnection connection = new ClientConnection(clientChannel);

        // The KEY insight of "attachment": we register this client's
        // channel for OP_READ, and attach its ClientConnection object to
        // the resulting SelectionKey. Every future event on this
        // channel arrives as a SelectionKey we can pull that SAME
        // ClientConnection back out of via key.attachment() -- this is
        // how the reactor finds "whose decoder/write-queue is this."
        clientChannel.register(selector, SelectionKey.OP_READ, connection);

        System.out.println("Client connected: " + clientChannel.getRemoteAddress());
    }

    /**
     * OP_READ fired: this client's socket has bytes available. Read them,
     * feed them to that client's own decoder, and dispatch every
     * complete command the decoder can extract.
     */
    private void handleRead(SelectionKey key) throws IOException {
        ClientConnection connection = (ClientConnection) key.attachment();
        SocketChannel channel = connection.channel();
        ByteBuffer readBuffer = connection.readBuffer();

        readBuffer.clear(); // reset position=0, limit=capacity, ready for a fresh read
        int bytesRead = channel.read(readBuffer);

        if (bytesRead == -1) {
            // -1 from a non-blocking read means the client closed the
            // connection (sent a FIN) -- not "no data right now," which
            // would be 0. This distinction matters: treating -1 like 0
            // would leave a dead connection registered forever.
            closeConnection(key);
            return;
        }

        if (bytesRead == 0) {
            // Genuinely nothing to read right now -- can happen, safe
            // to just return and wait for the next real OP_READ event.
            return;
        }

        readBuffer.flip(); // switch from "writing mode" to "reading mode": limit=position, position=0
        byte[] data = new byte[readBuffer.remaining()];
        readBuffer.get(data);

        // Feed raw bytes into THIS client's own decoder -- the exact
        // same feed()/tryDecode() pattern proven in RespDecoderTest and
        // EndToEndPipelineTest, just now fed by a real socket instead of
        // a String literal.
        connection.decoder().feed(data, data.length);
        List<RespValue> decodedCommands = connection.decoder().tryDecode();

        for (RespValue decoded : decodedCommands) {
            RespValue response = dispatchSafely(decoded);
            byte[] encoded = RespEncoder.encode(response);
            connection.queueWrite(encoded);
        }

        if (connection.hasPendingWrites()) {
            // We now have response bytes waiting to go out -- tell the
            // selector we're ALSO interested in OP_WRITE for this
            // channel, so we get notified when the socket has room to
            // accept them (see handleWrite below).
            key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
        }
    }

    /**
     * Dispatches a decoded command, catching any exception the dispatcher
     * or a command implementation might throw (e.g. the
     * IllegalArgumentException CommandDispatcher throws for malformed
     * argument types) and converting it into a RESP error response
     * instead of letting it propagate up into runEventLoop's generic
     * IOException handling -- that catch block is specifically for
     * socket-level I/O failures, not command-level logic errors, so we
     * handle this closer to where it actually happens.
     *
     * ALSO responsible for AOF logging: after a successful dispatch, if
     * the command that ran was mutating AND didn't itself return an
     * Error, we append it to the AOF log. Both conditions matter --
     * logging a command that FAILED validation (e.g. "SET" with too few
     * args, which never actually touched the store) would corrupt replay
     * by re-attempting something that never really happened the first
     * time either. We only log commands that had a REAL effect.
     */
    private RespValue dispatchSafely(RespValue decoded) {
        if (!(decoded instanceof RespValue.Array array) || array.items().isEmpty()) {
            return new RespValue.Error("ERR expected array command");
        }

        try {
            RespValue response = dispatcher.dispatch(array);

            if (!(response instanceof RespValue.Error)) {
                logToAofIfMutating(array);
            }

            return response;
        } catch (Exception e) {
            return new RespValue.Error("ERR " + e.getMessage());
        }
    }

    /**
     * Extracts the command name from the decoded array, looks up whether
     * that command is mutating via Command.isMutating(), and appends to
     * the AOF log if so. This duplicates a small amount of "find the
     * Command for this name" logic that CommandDispatcher also does
     * internally -- an accepted trade-off, since exposing
     * CommandDispatcher's internal name-to-Command map just for this one
     * check would leak an implementation detail the dispatcher should
     * otherwise keep private.
     */
    private void logToAofIfMutating(RespValue.Array commandArray) {
        List<RespValue> items = commandArray.items();
        if (items.isEmpty() || !(items.get(0) instanceof RespValue.BulkString nameValue)) {
            return;
        }

        boolean mutating = dispatcher.isMutatingCommand(nameValue.value());
        if (!mutating) {
            return;
        }

        String commandName = nameValue.value();
        List<String> args = items.subList(1, items.size()).stream()
            .filter(v -> v instanceof RespValue.BulkString)
            .map(v -> ((RespValue.BulkString) v).value())
            .toList();

        try {
            aofWriter.append(commandName, args);
        } catch (IOException e) {
            // A failure to WRITE to the AOF log is serious (durability
            // is now compromised for this command) but must NOT crash
            // the reactor thread or fail the client's request -- the
            // command already executed successfully in memory, and the
            // client already got a correct response. We log the failure
            // loudly to stderr so it's visible to an operator, but the
            // server keeps running. A production system might track
            // this as a health/alerting signal; for this project,
            // visibility via stderr is a reasonable, honest baseline.
            System.err.println("WARNING: failed to write to AOF: " + e.getMessage());
        }
    }

    /**
     * OP_WRITE fired: this client's socket now has room to accept more
     * bytes. Flush as much of the pending write queue as possible.
     */
    private void handleWrite(SelectionKey key) throws IOException {
        ClientConnection connection = (ClientConnection) key.attachment();
        boolean fullyFlushed = connection.flushWrites();

        if (fullyFlushed) {
            // Nothing left to write -- stop asking the selector to wake
            // us for OP_WRITE on this channel. Leaving OP_WRITE
            // registered with nothing to send would make select() wake
            // up constantly for no reason (the socket is ALWAYS
            // writable when idle), wasting CPU in a busy-loop.
            key.interestOps(SelectionKey.OP_READ);
        }
        // If not fully flushed, we leave OP_WRITE registered -- we'll
        // get called again next time the socket has more room.
    }

    private void closeConnection(SelectionKey key) {
        ClientConnection connection = (ClientConnection) key.attachment();
        if (connection != null) {
            connection.close();
        }
        key.cancel();
    }
}