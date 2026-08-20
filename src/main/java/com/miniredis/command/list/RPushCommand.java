package com.miniredis.command.list;

import com.miniredis.command.Command;
import com.miniredis.protocol.RespValue;
import com.miniredis.store.KeyValueStore;

import java.util.Deque;
import java.util.List;

/**
 * RPUSH key value [value2 value3 ...]
 *
 * Same as LPUSH but inserts at the TAIL. "RPUSH mylist a b c" results in
 * [a, b, c] -- each value appended after the previous one, so the final
 * order matches argument order exactly (unlike LPUSH, which reverses it).
 */
public final class RPushCommand implements Command {

    @Override
    public RespValue execute(List<String> args, KeyValueStore store) {
        if (args.size() < 2) {
            return new RespValue.Error("ERR wrong number of arguments for 'rpush' command");
        }

        String key = args.get(0);
        List<String> values = args.subList(1, args.size());

        // Reuses LPushCommand's helper -- the "resolve or create, check
        // for WRONGTYPE" logic is identical between LPUSH and RPUSH; only
        // WHICH end gets mutated differs.
        Deque<String> deque = LPushCommand.resolveOrCreateDeque(key, store);
        if (deque == null) {
            return new RespValue.Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        for (String value : values) {
            deque.addLast(value);
        }

        return new RespValue.Integer(deque.size());
    }
}