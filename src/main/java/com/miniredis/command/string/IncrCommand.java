package com.miniredis.command.string;

import com.miniredis.command.Command;
import com.miniredis.protocol.RespValue;
import com.miniredis.store.KeyValueStore;
import com.miniredis.store.StoredValue;

import java.util.List;
import java.util.Optional;

/**
 * INCR key
 *
 * Increments the integer value stored at key by 1, and returns the new
 * value. Three cases to handle correctly:
 *   1. Key doesn't exist       -> treat as 0, so result becomes 1.
 *   2. Key exists, holds "42"  -> parse it, add 1, store "43", return 43.
 *   3. Key exists, holds "abc" -> not a valid integer, real Redis returns
 *                                  a specific error rather than crashing
 *                                  or silently treating it as 0.
 */
public final class IncrCommand implements Command {

    @Override
    public RespValue execute(List<String> args, KeyValueStore store) {
        if (args.size() != 1) {
            return new RespValue.Error("ERR wrong number of arguments for 'incr' command");
        }

        String key = args.get(0);
        Optional<StoredValue> existing = store.get(key);

        long currentValue;
        if (existing.isEmpty()) {
            // Case 1: missing key behaves as if it held "0".
            currentValue = 0;
        } else {
            // Case 2/3: must be a StringValue holding a parseable integer.
            // Same WRONGTYPE pattern as GetCommand -- INCR against a list
            // is a type error, not something to silently coerce.
            String raw = switch (existing.get()) {
                case StoredValue.StringValue s -> s.value();
                case StoredValue.ListValue l -> null; // sentinel, handled below
            };

            if (raw == null) {
                return new RespValue.Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
            }

            try {
                currentValue = Long.parseLong(raw);
            } catch (NumberFormatException e) {
                // Case 3: this is real Redis's actual error message for
                // this exact situation -- matching it isn't cosmetic, a
                // client testing error-handling against this server would
                // check for this specific text.
                return new RespValue.Error("ERR value is not an integer or out of range");
            }
        }

        // Guard against overflow -- incrementing Long.MAX_VALUE would wrap
        // to a negative number silently in plain Java arithmetic, which
        // would be a genuinely confusing bug for anyone using INCR as a
        // counter. Real Redis explicitly rejects this case too.
        if (currentValue == Long.MAX_VALUE) {
            return new RespValue.Error("ERR increment or decrement would overflow");
        }

        long newValue = currentValue + 1;
        store.set(key, new StoredValue.StringValue(Long.toString(newValue)));

        return new RespValue.Integer(newValue);
    }
}