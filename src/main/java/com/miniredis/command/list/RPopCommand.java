package com.miniredis.command.list;

import com.miniredis.command.Command;
import com.miniredis.protocol.RespValue;
import com.miniredis.store.KeyValueStore;
import com.miniredis.store.StoredValue;

import java.util.Deque;
import java.util.List;
import java.util.Optional;

/**
 * RPOP key
 *
 * Mirror image of LPOP: removes and returns the element at the TAIL.
 * Same empty-list-deletes-the-key behavior applies.
 */
public final class RPopCommand implements Command {

    @Override
    public RespValue execute(List<String> args, KeyValueStore store) {
        if (args.size() != 1) {
            return new RespValue.Error("ERR wrong number of arguments for 'rpop' command");
        }

        String key = args.get(0);
        Optional<StoredValue> existing = store.get(key);

        if (existing.isEmpty()) {
            return new RespValue.NullBulk();
        }

        Deque<String> deque = switch (existing.get()) {
            case StoredValue.ListValue l -> l.value();
            case StoredValue.StringValue s -> null;
        };

        if (deque == null) {
            return new RespValue.Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        String value = deque.pollLast();
        if (value == null) {
            return new RespValue.NullBulk();
        }

        if (deque.isEmpty()) {
            store.del(key);
        }

        return new RespValue.BulkString(value);
    }
}