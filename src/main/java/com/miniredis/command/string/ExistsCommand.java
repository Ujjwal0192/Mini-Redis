package com.miniredis.command.string;

import com.miniredis.command.Command;
import com.miniredis.protocol.RespValue;
import com.miniredis.store.KeyValueStore;

import java.util.List;

/**
 * EXISTS key [key2 key3 ...]
 *
 * Like DEL, real Redis's EXISTS accepts multiple keys and returns a COUNT
 * of how many exist -- not a boolean. This trips people up if they assume
 * it returns 0/1 like a typical "exists" check; with multiple keys it can
 * return any number up to the count of keys given, including counting the
 * SAME key twice if it's passed twice.
 */
public final class ExistsCommand implements Command {

    @Override
    public boolean isMutating() {
        return false;
    }

    @Override
    public RespValue execute(List<String> args, KeyValueStore store) {
        if (args.isEmpty()) {
            return new RespValue.Error("ERR wrong number of arguments for 'exists' command");
        }

        long existingCount = 0;
        for (String key : args) {
            if (store.exists(key)) {
                existingCount++;
            }
        }

        return new RespValue.Integer(existingCount);
    }
}