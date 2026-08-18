package com.miniredis.command.string;

import com.miniredis.command.Command;
import com.miniredis.protocol.RespValue;
import com.miniredis.store.KeyValueStore;

import java.util.List;

/**
 * EXPIRE key seconds
 *
 * Sets a TTL on an existing key. Returns 1 if the timeout was set
 * (the key existed), 0 if it wasn't (the key didn't exist) -- matching
 * real Redis's convention of using an Integer reply as a boolean-ish
 * success signal, same pattern you've already seen in DEL/EXISTS.
 */
public final class ExpireCommand implements Command {

    @Override
    public RespValue execute(List<String> args, KeyValueStore store) {
        if (args.size() != 2) {
            return new RespValue.Error("ERR wrong number of arguments for 'expire' command");
        }

        String key = args.get(0);
        long seconds;
        try {
            seconds = Long.parseLong(args.get(1));
        } catch (NumberFormatException e) {
            return new RespValue.Error("ERR value is not an integer or out of range");
        }

        // KeyValueStore.expire() already encodes the exact semantics we
        // need: returns false if the key doesn't exist, true (and
        // actually sets the TTL) if it does. This command's only job is
        // translating that boolean into the RESP Integer 0/1 convention.
        boolean wasSet = store.expire(key, seconds);
        return new RespValue.Integer(wasSet ? 1 : 0);
    }
}
