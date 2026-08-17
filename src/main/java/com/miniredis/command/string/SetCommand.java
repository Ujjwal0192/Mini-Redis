package com.miniredis.command.string;

import com.miniredis.command.Command;
import com.miniredis.protocol.RespValue;
import com.miniredis.store.KeyValueStore;
import com.miniredis.store.StoredValue;

import java.util.List;

/**
 * SET key value [EX seconds]
 *
 * Real Redis's SET supports many more options (NX, XX, PX, KEEPTTL, GET...)
 * -- we're implementing the subset your feature list specifies: plain SET,
 * and SET with EX for setting a TTL in the same call. Scoping to exactly
 * what's needed, rather than the full real-Redis surface, is a deliberate
 * choice worth being able to defend: implementing every SET flag adds a
 * lot of argument-parsing complexity for options nothing else in this
 * project (AOF replay, LRU, etc) actually exercises or needs.
 */
public final class SetCommand implements Command {

    @Override
    public RespValue execute(List<String> args, KeyValueStore store) {
        // Minimum valid form is "SET key value" -> 2 args after the
        // command name is stripped by CommandDispatcher.
        if (args.size() < 2) {
            return new RespValue.Error("ERR wrong number of arguments for 'set' command");
        }

        String key = args.get(0);
        String value = args.get(1);

        store.set(key, new StoredValue.StringValue(value));

        // Optional EX seconds -- "SET key value EX 10" has args =
        // ["key", "value", "EX", "10"], so EX sits at index 2, the
        // seconds value at index 3.
        if (args.size() >= 4 && args.get(2).equalsIgnoreCase("EX")) {
            long seconds;
            try {
                seconds = Long.parseLong(args.get(3));
            } catch (NumberFormatException e) {
                return new RespValue.Error("ERR value is not an integer or out of range");
            }
            if (seconds <= 0) {
                return new RespValue.Error("ERR invalid expire time in 'set' command");
            }
            // store.set() above already cleared any prior TTL -- this call
            // sets a fresh one, which is exactly the semantics we want:
            // the key we just wrote now expires `seconds` from now.
            store.expire(key, seconds);
        }

        // Real Redis returns +OK for a successful SET -- a SimpleString,
        // not a BulkString. redis-cli renders SimpleString("OK") as
        // plain OK, and a BulkString("OK") would actually render
        // differently (quoted), so this distinction is directly visible
        // to anyone testing against redis-cli, not just an internal detail.
        return new RespValue.SimpleString("OK");
    }
}