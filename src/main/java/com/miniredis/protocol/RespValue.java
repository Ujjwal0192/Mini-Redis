package com.miniredis.protocol;

import java.util.List;

/**
 * Represents any value in the RESP (REdis Serialization Protocol) wire format.
 * RESP has 5 types, each identified by its first byte on the wire:
 *   +  Simple String   -> "+OK\r\n"
 *   -  Error           -> "-ERR unknown command\r\n"
 *   :  Integer         -> ":1000\r\n"
 *   $  Bulk String     -> "$5\r\nhello\r\n"  (length-prefixed, binary safe)
 *   *  Array           -> "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"
 *
 * sealed means only the classes listed in `permits` can implement this
 * interface. The compiler then knows the exhaustive set of subtypes, so a
 * switch expression over RespValue doesn't need a default branch -- if you
 * add a 6th type later, every switch that handles RespValue will fail to
 * compile until you handle the new case. That's a deliberate safety net.
 */
public sealed interface RespValue
        permits RespValue.SimpleString, RespValue.Error, RespValue.Integer,
                 RespValue.BulkString, RespValue.Array, RespValue.NullBulk {

    record SimpleString(String value) implements RespValue {}

    record Error(String message) implements RespValue {}

    record Integer(long value) implements RespValue {}

    record BulkString(String value) implements RespValue {}

    /**
     * A bulk string that is null -- RESP's way of representing "no value",
     * e.g. GET on a missing key returns $-1\r\n, not an empty string.
     * This is its own type, not BulkString(null), so the compiler forces
     * every consumer to handle "key doesn't exist" as a distinct case
     * instead of accidentally NPEing on .value().
     */
    record NullBulk() implements RespValue {}

    record Array(List<RespValue> items) implements RespValue {}
}