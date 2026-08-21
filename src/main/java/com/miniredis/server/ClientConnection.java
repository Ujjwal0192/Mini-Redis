package com.miniredis.server;

import com.miniredis.protocol.RespDecoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.Queue;

/**
 * Holds all per-client state for one connected socket. One instance of
 * this class exists per connected client, for the lifetime of that
 * client's connection.
 *
 * WHY THIS CLASS EXISTS AT ALL: in a thread-per-connection server, "per
 * client state" would just be local variables on that client's dedicated
 * thread's stack -- the thread itself IS the state container. In a
 * single-threaded reactor, there is no dedicated thread per client, so
 * anything that needs to persist BETWEEN separate OP_READ/OP_WRITE events
 * for the same client (the partially-filled RespDecoder buffer, pending
 * outbound bytes) has to live somewhere explicit. This class is that
 * "somewhere."
 */
public final class ClientConnection {

    private final SocketChannel channel;

    // Fixed-size scratch buffer the reactor reads INTO on every OP_READ
    // event. 8KB is a reasonable default -- large enough that most
    // realistic commands fit in one read, small enough that having one
    // per connection doesn't waste much memory even with many clients.
    // This buffer's contents get copied out to the decoder immediately
    // after each read (see ReactorServer) -- it's reused every cycle,
    // never accumulates state itself.
    private final ByteBuffer readBuffer = ByteBuffer.allocate(8192);

    // This client's OWN decoder instance -- see the class-level note.
    // This is what remembers "I have 3 bytes of a partial command left
    // over from last time" across multiple read events.
    private final RespDecoder decoder = new RespDecoder();

    // Bytes queued to be written back to this client but not yet fully
    // flushed to the socket. A Queue<ByteBuffer> rather than a single
    // buffer because multiple responses can pile up faster than the OS
    // socket buffer drains them (e.g. a burst of pipelined commands
    // producing several responses before the client reads any of them).
    private final Queue<ByteBuffer> pendingWrites = new ArrayDeque<>();

    public ClientConnection(SocketChannel channel) {
        this.channel = channel;
    }

    public SocketChannel channel() {
        return channel;
    }

    public RespDecoder decoder() {
        return decoder;
    }

    public ByteBuffer readBuffer() {
        return readBuffer;
    }

    /**
     * Queues bytes to be sent to this client. Does NOT write to the
     * socket directly -- actual writing happens in ReactorServer's
     * flushPendingWrites(), triggered by an OP_WRITE event. Separating
     * "queue this response" from "actually send bytes" is what lets the
     * reactor handle a socket that isn't ready to accept more data yet
     * without blocking the whole event loop waiting for it to become
     * ready.
     */
    public void queueWrite(byte[] data) {
        pendingWrites.add(ByteBuffer.wrap(data));
    }

    public boolean hasPendingWrites() {
        return !pendingWrites.isEmpty();
    }

    /**
     * Attempts to flush as many queued bytes as the socket will currently
     * accept, without blocking. Returns true if everything queued has now
     * been fully written (meaning the reactor can stop listening for
     * OP_WRITE on this channel until more data is queued); false if some
     * data is still waiting (meaning OP_WRITE interest must stay
     * registered so we get called again once the socket has more room).
     */
    public boolean flushWrites() throws IOException {
        while (!pendingWrites.isEmpty()) {
            ByteBuffer buffer = pendingWrites.peek();
            channel.write(buffer); // non-blocking: writes as much as the OS buffer currently accepts

            if (buffer.hasRemaining()) {
                // The OS socket send buffer is full right now -- this
                // ByteBuffer has bytes left over. Stop here; we'll retry
                // this SAME buffer (with its remaining, un-written bytes)
                // the next time OP_WRITE fires for this channel.
                return false;
            }
            // This buffer is fully written -- remove it and move to the
            // next queued response, if any.
            pendingWrites.poll();
        }
        return true; // queue is now empty, everything has been sent
    }

    public void close() {
        try {
            channel.close();
        } catch (IOException e) {
            // Closing an already-broken socket can throw -- there's
            // nothing meaningful to do about a failure to close a
            // connection we're discarding anyway, so we swallow this
            // specific, expected exception rather than letting it
            // propagate and interrupt whatever cleanup loop called us.
        }
    }
}