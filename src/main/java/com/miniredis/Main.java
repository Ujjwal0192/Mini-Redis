package com.miniredis;

import com.miniredis.command.CommandDispatcher;
import com.miniredis.server.ReactorServer;
import com.miniredis.store.ExpiryEngine;
import com.miniredis.store.KeyValueStore;

import java.io.IOException;

/**
 * Entry point. Wires together every layer built so far -- store,
 * dispatcher, expiry engine, reactor -- and starts the server.
 *
 * Deliberately thin: this class contains almost no logic of its own.
 * Its only job is object construction and dependency wiring (this is
 * "dependency injection" in its simplest possible form -- no framework,
 * just passing constructed objects into other constructors by hand,
 * which is exactly what frameworks like Spring automate at larger
 * scale). Keeping Main this thin means every other class can be
 * constructed and tested in isolation, exactly as you've been doing in
 * every test file so far -- none of them needed Main to exist at all.
 */
public final class Main {

    // Maximum number of keys before LRU eviction kicks in. Hardcoded for
    // now -- a real production system would make this configurable via
    // an environment variable or config file, which is a natural later
    // improvement once the core server is proven working end-to-end.
    private static final int MAX_KEYS = 10_000;

    public static void main(String[] args) {
        java.nio.file.Path aofPath = java.nio.file.Path.of("mini-redis.aof");

        KeyValueStore store = new KeyValueStore(MAX_KEYS);
        CommandDispatcher dispatcher = new CommandDispatcher(store);
        ExpiryEngine expiryEngine = new ExpiryEngine(store);
        com.miniredis.persistence.AofWriter aofWriter =
            new com.miniredis.persistence.AofWriter(aofPath);

        try {
            // CRITICAL ORDERING: replay must fully complete BEFORE the
            // reactor starts accepting connections. If a client could
            // connect mid-replay, it might read incomplete state, or a
            // fresh client write could race with replay and end up
            // applied out of order relative to historical commands.
            // Doing this synchronously, here, before ReactorServer even
            // exists, makes that ordering structurally guaranteed rather
            // than something we have to be careful about elsewhere.
            com.miniredis.persistence.AofReplayer replayer =
                new com.miniredis.persistence.AofReplayer(aofPath, dispatcher);
            int replayedCount = replayer.replay();
            System.out.println("Replayed " + replayedCount + " commands from AOF");
        } catch (IOException e) {
            System.err.println("Failed to replay AOF: " + e.getMessage());
            System.exit(1);
        }

        ReactorServer server = new ReactorServer(dispatcher, expiryEngine, aofWriter);

        try {
            // start() runs the event loop forever -- this call does not
            // return under normal operation. Everything after it in this
            // method only runs if start() throws.
            server.start();
        } catch (IOException e) {
            System.err.println("Failed to start Mini Redis: " + e.getMessage());
            System.exit(1);
        }
    }
}