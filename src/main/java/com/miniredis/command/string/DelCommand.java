package com.miniredis.command.string;

import com.miniredis.command.Command;
import com.miniredis.protocol.RespValue;
import com.miniredis.store.KeyValueStore;

import java.util.List;

/**
 * DEL key [key2 key3 ...]
 *
 * Real Redis's DEL accepts multiple keys in one call and returns how many
 * were actually deleted. We support that -- it's not extra complexity,
 * just a loop, and it's a real behavior difference from "DEL only takes
 * one key" that redis-cli users would notice immediately.
 */
public final class DelCommand implements Command {

    @Override
    public RespValue execute(List<String> args, KeyValueStore store) {
        if (args.isEmpty()) {
            return new RespValue.Error("ERR wrong number of arguments for 'del' command");
        }

        long deletedCount = 0;
        for (String key : args) {
            if (store.del(key)) {
                deletedCount++;
            }
        }

        // Real Redis returns an Integer reply: how many keys were
        // actually removed (deleting a non-existent key doesn't count).
        return new RespValue.Integer(deletedCount);
    }
}