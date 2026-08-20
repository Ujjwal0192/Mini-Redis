package com.miniredis.command.list;

import com.miniredis.command.Command;
import com.miniredis.protocol.RespValue;
import com.miniredis.store.KeyValueStore;
import com.miniredis.store.StoredValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * LRANGE key start stop
 *
 * Returns elements from index `start` to `stop`, INCLUSIVE on both ends,
 * with real Redis's negative-index convention: -1 is the last element,
 * -2 the second-to-last, and so on. Out-of-range indices are clamped
 * rather than erroring -- "LRANGE mylist 0 9999" on a 3-element list
 * just returns all 3 elements, matching real Redis exactly.
 */
public final class LRangeCommand implements Command {

  @Override
    public boolean isMutating() {
        return false;
    }

    @Override
    public RespValue execute(List<String> args, KeyValueStore store) {
        if (args.size() != 3) {
            return new RespValue.Error("ERR wrong number of arguments for 'lrange' command");
        }

        String key = args.get(0);
        int startArg;
        int stopArg;
        try {
            startArg = Integer.parseInt(args.get(1));
            stopArg = Integer.parseInt(args.get(2));
        } catch (NumberFormatException e) {
            return new RespValue.Error("ERR value is not an integer or out of range");
        }

        Optional<StoredValue> existing = store.get(key);
        if (existing.isEmpty()) {
            // Real Redis returns an empty array for LRANGE on a missing
            // key -- NOT an error, and NOT nil. This is different from
            // GET's "missing key" behavior, worth noting explicitly.
            return new RespValue.Array(List.of());
        }

        List<String> elements = switch (existing.get()) {
            case StoredValue.ListValue l -> new ArrayList<>(l.value());
            case StoredValue.StringValue s -> null;
        };

        if (elements == null) {
            return new RespValue.Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        int size = elements.size();
        int start = normalizeIndex(startArg, size);
        int stop = normalizeIndex(stopArg, size);

        // Clamp into valid bounds AFTER negative-index conversion --
        // real Redis clamps rather than errors on out-of-range values,
        // and an empty result (rather than an error) is correct when the
        // range doesn't overlap the list at all.
        start = Math.max(start, 0);
        stop = Math.min(stop, size - 1);

        if (start > stop || size == 0) {
            return new RespValue.Array(List.of());
        }

        List<RespValue> result = new ArrayList<>();
        for (int i = start; i <= stop; i++) {
            result.add(new RespValue.BulkString(elements.get(i)));
        }

        return new RespValue.Array(result);
    }

    /**
     * Converts a possibly-negative Redis-style index into a real,
     * positive list index. -1 means "last element," i.e. size - 1.
     * A negative index that's still out of range after conversion
     * (e.g. -100 on a 3-element list) is left negative here deliberately
     * -- the caller's Math.max(start, 0) clamp handles that afterward,
     * keeping this method's one job simple: "convert," not "convert AND
     * clamp."
     */
    private int normalizeIndex(int index, int size) {
        return index < 0 ? size + index : index;
    }
}