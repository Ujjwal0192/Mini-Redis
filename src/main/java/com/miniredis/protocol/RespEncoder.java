package com.miniredis.protocol;

import java.nio.charset.StandardCharsets;

/**
 * Converts a RespValue (our Java-side representation) into the exact byte
 * sequence Redis's wire protocol expects. This is the direction the server
 * uses when sending a response back to redis-cli.
 *
 * Every RESP type ends its line(s) with CRLF ("\r\n") -- not just "\n".
 * This is non-negotiable: redis-cli's own parser expects CRLF, and if you
 * emit bare "\n", real Redis tooling will hang waiting for the rest of
 * the terminator or misparse the frame entirely.
 */
public final class RespEncoder {

    private static final String CRLF = "\r\n";

    // Prevent instantiation -- this class is a pure set of static functions,
    // there's no state to hold, so an instance would serve no purpose.
    private RespEncoder() {}

    /**
     * Entry point: dispatches to the correct encoding routine based on the
     * runtime type of the sealed RespValue. This switch is exhaustive
     * because RespValue is sealed -- the compiler checked at RespValue's
     * definition site that these are the only 6 possible subtypes, so no
     * default branch is needed here.
     */
    public static byte[] encode(RespValue value) {
        String wire = switch (value) {
            case RespValue.SimpleString s -> encodeSimpleString(s);
            case RespValue.Error e -> encodeError(e);
            case RespValue.Integer i -> encodeInteger(i);
            case RespValue.BulkString b -> encodeBulkString(b);
            case RespValue.NullBulk n -> encodeNullBulk();
            case RespValue.Array a -> encodeArray(a);
        };
        return wire.getBytes(StandardCharsets.UTF_8);
    }

    private static String encodeSimpleString(RespValue.SimpleString s) {
        // Wire format: +OK\r\n
        // Simple strings can't contain \r or \n themselves -- that's what
        // bulk strings are for (binary-safe, length-prefixed). We don't
        // validate that here; real Redis commands only ever construct
        // SimpleString with known-safe literals like "OK", "PONG".
        return "+" + s.value() + CRLF;
    }

    private static String encodeError(RespValue.Error e) {
        // Wire format: -ERR wrong number of arguments\r\n
        // redis-cli specifically checks for the leading '-' to decide
        // whether to print the response in red as an error.
        return "-" + e.message() + CRLF;
    }

    private static String encodeInteger(RespValue.Integer i) {
        // Wire format: :1000\r\n
        return ":" + i.value() + CRLF;
    }

    private static String encodeBulkString(RespValue.BulkString b) {
        // Wire format: $5\r\nhello\r\n
        // The length prefix is in BYTES, not characters -- for pure ASCII
        // these are the same, but it matters the moment you store UTF-8
        // multi-byte characters. We compute byte length explicitly rather
        // than String.length() (which counts UTF-16 code units) to stay
        // correct for non-ASCII values.
        byte[] bytes = b.value().getBytes(StandardCharsets.UTF_8);
        return "$" + bytes.length + CRLF + b.value() + CRLF;
    }

    private static String encodeNullBulk() {
        // Wire format: $-1\r\n
        // This is how Redis says "this bulk string does not exist" --
        // e.g. GET on a missing key. Note there's no second line here;
        // -1 as the length IS the entire signal, there's no body to follow.
        return "$-1" + CRLF;
    }

    private static String encodeArray(RespValue.Array a) {
        // Wire format: *2\r\n<encoded item 1><encoded item 2>
        // Each item is itself a fully-encoded RespValue, recursively.
        // This recursion is what lets RESP represent nested structures
        // (an array containing arrays), even though we won't need that
        // nesting for the commands in scope here.
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(a.items().size()).append(CRLF);
        for (RespValue item : a.items()) {
            // Recursive call -- reuses encode() so every branch above
            // (including nested arrays) is handled uniformly.
            sb.append(new String(encode(item), StandardCharsets.UTF_8));
        }
        return sb.toString();
    }
}