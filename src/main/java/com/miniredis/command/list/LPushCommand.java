package com.miniredis.command.list;

import com.miniredis.command.Command;
import com.miniredis.protocol.RespValue;
import com.miniredis.store.KeyValueStore;
import com.miniredis.store.StoredValue;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Optional;

/**
 * LPUSH key value [value2 value3 ...]
 *
 * Inserts each value at the HEAD of the list, one at a time, in argument
 * order. This means "LPUSH mylist a b c" results in [c, b, a] -- each
 * subsequent value gets pushed further toward the head than the one
 * before it. Returns the list's new length.
 */
public final class LPushCommand implements Command {

    @Override
    public RespValue execute(List<String> args, KeyValueStore store) {
        if (args.size() < 2) {
            return new RespValue.Error("ERR wrong number of arguments for 'lpush' command");
        }

        String key = args.get(0);
        List<String> values = args.subList(1, args.size());

        Deque<String> deque = resolveOrCreateDeque(key, store);
        if (deque == null) {
            return new RespValue.Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        for (String value : values) {
            deque.addFirst(value);
        }

        return new RespValue.Integer(deque.size());
    }

    /**
     * Fetches the existing Deque backing this key, creating and storing a
     * new empty one if the key doesn't exist yet. Returns null if the key
     * exists but holds a StringValue (a type error), which callers must
     * check for.
     *
     * IMPORTANT: when the key already exists, this returns the SAME
     * Deque object reference already living inside the store's
     * StoredValue.ListValue -- mutating it in place (via addFirst below)
     * is sufficient, we do NOT call store.set() again for the existing
     * case. This is deliberate: store.set() always clears any TTL the
     * key had (correct behavior for SET, wrong for LPUSH -- pushing onto
     * a list must not silently remove its expiry). Only the "key is
     * brand new" branch calls store.set(), because there's no prior TTL
     * to accidentally clear in that case anyway.
     */
    static Deque<String> resolveOrCreateDeque(String key, KeyValueStore store) {
        Optional<StoredValue> existing = store.get(key);

        if (existing.isEmpty()) {
            Deque<String> newDeque = new ArrayDeque<>();
            store.set(key, new StoredValue.ListValue(newDeque));
            return newDeque;
        }

        return switch (existing.get()) {
            case StoredValue.ListValue l -> l.value();
            case StoredValue.StringValue s -> null; // signals WRONGTYPE to the caller
        };
    }
}