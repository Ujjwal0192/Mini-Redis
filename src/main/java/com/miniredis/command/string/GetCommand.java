package com.miniredis.command.string;

import com.miniredis.command.Command;
import com.miniredis.protocol.RespValue;
import com.miniredis.store.KeyValueStore;
import com.miniredis.store.StoredValue;

import java.util.List;
import java.util.Optional;

/**
 * GET key
 *
 * Returns the string value if present, or a RESP null bulk string if the
 * key doesn't exist (or has expired -- KeyValueStore.get() already applies
 * passive expiry before returning, so this command doesn't need to know
 * or care about TTL at all).
 */
public final class GetCommand implements Command {

    @Override
    public boolean isMutating() {
        return false;
    }

    @Override
    public RespValue execute(List<String> args, KeyValueStore store) {
        if (args.size() != 1) {
            return new RespValue.Error("ERR wrong number of arguments for 'get' command");
        }

        String key = args.get(0);
        Optional<StoredValue> stored = store.get(key);

        if (stored.isEmpty()) {
            // This is exactly why NullBulk exists as its own RespValue
            // type: GET on a missing key must produce $-1\r\n on the
            // wire, which is what lets redis-cli print (nil) instead of
            // an empty string.
            return new RespValue.NullBulk();
        }

        // GET only makes sense against a string value. If the key holds
        // a list instead, real Redis returns a specific WRONGTYPE error
        // rather than silently coercing or crashing -- we match that.
        return switch (stored.get()) {
            case StoredValue.StringValue s -> new RespValue.BulkString(s.value());
            case StoredValue.ListValue l -> new RespValue.Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value");
        };
    }
}