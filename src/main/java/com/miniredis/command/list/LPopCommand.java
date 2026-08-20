package com.miniredis.command.list;

import com.miniredis.command.Command;
import com.miniredis.protocol.RespValue;
import com.miniredis.store.KeyValueStore;
import com.miniredis.store.StoredValue;

import java.util.Deque;
import java.util.List;
import java.util.Optional;

/**
 * LPOP key
 *
 * Removes and returns the element at the HEAD of the list. Returns nil
 * if the key doesn't exist. If removing this element empties the list,
 * the key itself is deleted -- matching real Redis, where an empty list
 * is not a valid stored value; the key simply ceases to exist.
 */
public final class LPopCommand implements Command {

    @Override
    public RespValue execute(List<String> args, KeyValueStore store) {
        if (args.size() != 1) {
            return new RespValue.Error("ERR wrong number of arguments for 'lpop' command");
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

        // pollFirst() returns null if the deque is empty rather than
        // throwing -- but a stored ListValue should never actually be
        // empty in practice, since we delete the key the moment a pop
        // empties it (see below). This is still the correct, safe method
        // to call rather than removeFirst() (which WOULD throw on empty),
        // as defense against that invariant ever being violated.
        String value = deque.pollFirst();
        if (value == null) {
            return new RespValue.NullBulk();
        }

        if (deque.isEmpty()) {
            // The list is now empty -- remove the key entirely, matching
            // real Redis's "empty lists don't exist" behavior.
            store.del(key);
        }

        return new RespValue.BulkString(value);
    }
}