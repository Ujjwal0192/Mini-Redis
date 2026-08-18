package com.miniredis.command.string;

import com.miniredis.command.Command;
import com.miniredis.protocol.RespValue;
import com.miniredis.store.KeyValueStore;

import java.util.List;

/**
 * TTL key
 *
 * Returns remaining seconds until expiry, using real Redis's exact
 * sentinel convention (already implemented in KeyValueStore.ttlSeconds()
 * back when the store was built):
 *   -2  key doesn't exist
 *   -1  key exists but has no TTL
 *   >=0 remaining seconds
 *
 * This command does zero translation work beyond arg validation --
 * KeyValueStore already returns exactly the number RESP needs to send.
 */
public final class TtlCommand implements Command {

    @Override
    public boolean isMutating() {
        return false;
    }

    @Override
    public RespValue execute(List<String> args, KeyValueStore store) {
        if (args.size() != 1) {
            return new RespValue.Error("ERR wrong number of arguments for 'ttl' command");
        }

        String key = args.get(0);
        long ttl = store.ttlSeconds(key);
        return new RespValue.Integer(ttl);
    }
}