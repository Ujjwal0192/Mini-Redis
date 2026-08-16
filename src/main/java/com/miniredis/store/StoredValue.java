package com.miniredis.store;

import java.util.Deque;

/**
 * Represents the value half of a key-value pair in the store, independent
 * of RESP wire format. This is a deliberate separation: RespValue models
 * "what goes on the wire," StoredValue models "what's actually held in
 * memory." A stored list, for instance, is a java.util.Deque internally --
 * it only becomes a RespValue.Array when a command like LRANGE serializes
 * it for a response. Conflating these two concerns would make the store
 * depend on wire-format details it has no business knowing about.
 *
 * sealed, same reasoning as RespValue: the store only ever holds one of
 * these two shapes, and any switch over StoredValue is compiler-checked
 * for exhaustiveness.
 */
public sealed interface StoredValue permits StoredValue.StringValue, StoredValue.ListValue {

    record StringValue(String value) implements StoredValue {}

    /**
     * Deque, not ArrayList or LinkedList directly -- Deque is the
     * interface that exposes O(1) addFirst/addLast/removeFirst/removeLast,
     * which is exactly what LPUSH/RPUSH/LPOP/RPOP need. We'll back it with
     * ArrayDeque at construction time (in KeyValueStore), since
     * ArrayDeque gives O(1) amortized operations at both ends without the
     * per-node allocation overhead of a LinkedList.
     */
    record ListValue(Deque<String> value) implements StoredValue {}
}