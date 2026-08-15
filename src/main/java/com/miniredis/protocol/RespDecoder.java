package com.miniredis.protocol;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Converts raw bytes read from a client socket into RespValue objects.
 *
 * This class is STATEFUL and belongs to exactly one client connection.
 * Each call to feed() appends newly-read bytes to an internal buffer, then
 * tries to extract as many complete RESP frames as currently exist in that
 * buffer. Bytes belonging to an incomplete frame are left untouched in the
 * buffer for the next feed() call to pick up.
 *
 * Why not decode directly off the NIO ByteBuffer passed by the reactor?
 * Because the reactor's read buffer is reused/cleared on every read cycle.
 * If a frame is split across two reads, we need OUR OWN buffer that
 * persists between reactor read events -- this class owns that buffer.
 */
public final class RespDecoder {

    // Growing buffer of bytes received but not yet fully parsed into a
    // RespValue. StringBuilder-of-bytes doesn't exist in Java, so we use
    // a manually managed byte array with a cursor -- this mirrors what a
    // real ring buffer / accumulator would do, just simplified.
    private byte[] buffer = new byte[0];

    // How many bytes at the START of `buffer` have already been consumed
    // by a successful parse but not yet physically removed (compacted).
    // We compact lazily rather than on every parse for efficiency.
    private int consumedUpTo = 0;

    /**
     * Appends newly read bytes to the internal buffer.
     * Call this once per socket read event, BEFORE calling tryDecode().
     */
    public void feed(byte[] newBytes, int length) {
        compactIfNeeded();
        byte[] combined = new byte[remainingLength() + length];
        System.arraycopy(buffer, consumedUpTo, combined, 0, remainingLength());
        System.arraycopy(newBytes, 0, combined, remainingLength(), length);
        buffer = combined;
        consumedUpTo = 0;
    }

    /**
     * Attempts to extract ALL complete RESP frames currently available in
     * the buffer. Returns an empty list if there isn't even one complete
     * frame yet (pure partial-frame case). Returns multiple entries if the
     * buffer contained pipelined commands.
     *
     * This is the method that solves BOTH problems described above: it
     * loops (handles pipelining) and stops cleanly on incomplete data
     * (handles partial frames) without throwing.
     */
    public List<RespValue> tryDecode() {
        List<RespValue> results = new ArrayList<>();
        while (true) {
            Optional<ParseResult> result = parseOne(consumedUpTo);
            if (result.isEmpty()) {
                // Not enough bytes yet for even one more complete frame --
                // stop here and wait for the next feed() call.
                break;
            }
            results.add(result.get().value());
            consumedUpTo = result.get().nextIndex();
        }
        return results;
    }

    // A single parse attempt's outcome: the decoded value, plus the index
    // in `buffer` where the NEXT frame would start. Bundling these together
    // avoids needing a mutable "position" field shared across methods.
   private record ParseResult(RespValue value, int nextIndex) {}

    /**
     * Tries to parse exactly one complete RespValue starting at `pos`.
     * Returns Optional.empty() if the bytes from pos to buffer end don't
     * yet form a complete value (i.e., we need more bytes from the socket).
     */
    private Optional<ParseResult> parseOne(int pos) {
        if (pos >= buffer.length) {
            return Optional.empty();
        }
        byte typeByte = buffer[pos];
        return switch (typeByte) {
            case '+' -> parseSimpleString(pos + 1);
            case '-' -> parseError(pos + 1);
            case ':' -> parseInteger(pos + 1);
            case '$' -> parseBulkString(pos + 1);
            case '*' -> parseArray(pos + 1);
            default -> throw new ProtocolException(
                "Unknown RESP type byte: " + (char) typeByte);
        };
    }

    /**
     * Finds the index of the next CRLF ("\r\n") at or after `from`.
     * Returns -1 if no complete line ending exists yet in the buffer --
     * this is the signal that we have a partial frame and must wait for
     * more bytes.
     */
    private int findCrlf(int from) {
        for (int i = from; i < buffer.length - 1; i++) {
            if (buffer[i] == '\r' && buffer[i + 1] == '\n') {
                return i;
            }
        }
        return -1;
    }

    private String sliceAsString(int from, int to) {
        return new String(buffer, from, to - from, StandardCharsets.UTF_8);
    }

    // +OK\r\n -- everything up to CRLF is the string, verbatim.
    private Optional<ParseResult> parseSimpleString(int pos) {
        int crlf = findCrlf(pos);
        if (crlf == -1) return Optional.empty();
        String value = sliceAsString(pos, crlf);
        return Optional.of(new ParseResult(new RespValue.SimpleString(value), crlf + 2));
    }

    // -ERR message\r\n
    private Optional<ParseResult> parseError(int pos) {
        int crlf = findCrlf(pos);
        if (crlf == -1) return Optional.empty();
        String value = sliceAsString(pos, crlf);
        return Optional.of(new ParseResult(new RespValue.Error(value), crlf + 2));
    }

    // :1000\r\n
    private Optional<ParseResult> parseInteger(int pos) {
        int crlf = findCrlf(pos);
        if (crlf == -1) return Optional.empty();
        String digits = sliceAsString(pos, crlf);
        long value;
        try {
            value = Long.parseLong(digits);
        } catch (NumberFormatException e) {
            throw new ProtocolException("Invalid integer: " + digits);
        }
        return Optional.of(new ParseResult(new RespValue.Integer(value), crlf + 2));
    }

    /**
     * $5\r\nhello\r\n
     * Two-stage parse: first read the length line, THEN check whether
     * `length` bytes plus the trailing CRLF actually exist yet. This is
     * the classic partial-frame trap -- you can have the length line
     * complete ("$1000000\r\n" fully arrived) while the 1MB body hasn't.
     */
    private Optional<ParseResult> parseBulkString(int pos) {
        int lengthLineEnd = findCrlf(pos);
        if (lengthLineEnd == -1) return Optional.empty(); // length line itself incomplete

        String lengthStr = sliceAsString(pos, lengthLineEnd);
        int length;
        try {
            length = Integer.parseInt(lengthStr);
        } catch (NumberFormatException e) {
            throw new ProtocolException("Invalid bulk string length: " + lengthStr);
        }

        if (length < -1) {
            // RESP only ever uses -1 to mean "null". Anything else negative
            // is not a value any correct client would send.
            throw new ProtocolException("Invalid bulk string length: " + length);
        }

        if (length == -1) {
            // $-1\r\n -- null bulk string, no body follows at all.
            return Optional.of(new ParseResult(new RespValue.NullBulk(), lengthLineEnd + 2));
        }

        int bodyStart = lengthLineEnd + 2;
        int bodyEnd = bodyStart + length; // exclusive
        int frameEnd = bodyEnd + 2;       // +2 for the body's trailing CRLF

        if (frameEnd > buffer.length) {
            // We know the length, but the body (or its trailing CRLF)
            // hasn't fully arrived yet. Wait for more bytes.
            return Optional.empty();
        }

        // Strict validation: the two bytes immediately after the declared
        // body must actually BE \r\n. Previously this was assumed, never
        // checked -- if the length was wrong (client bug, or a bug
        // elsewhere in our own code composing a command), we'd silently
        // slice the wrong bytes as the value and leave the cursor
        // misaligned for every frame after this one, corrupting the
        // entire rest of the stream with no clear error at the point
        // of failure. Checking here turns a silent, delayed corruption
        // into an immediate, precise failure.
        if (buffer[bodyEnd] != '\r' || buffer[bodyEnd + 1] != '\n') {
            throw new ProtocolException(
                "Malformed bulk string: expected CRLF after " + length + " bytes");
        }

        String value = sliceAsString(bodyStart, bodyEnd);
        return Optional.of(new ParseResult(new RespValue.BulkString(value), frameEnd));
    }

    /**
     * *2\r\n<item1><item2>
     * Recursive: after reading the count, repeatedly call parseOne() for
     * each element. If ANY element is incomplete, the whole array parse
     * fails as incomplete too -- we must NOT partially consume an array,
     * or we'd desync the stream (the caller would think a full frame was
     * consumed when only some of its elements actually were).
     */
    private Optional<ParseResult> parseArray(int pos) {
        int countLineEnd = findCrlf(pos);
        if (countLineEnd == -1) return Optional.empty();

        String countStr = sliceAsString(pos, countLineEnd);
        int count;
        try {
            count = Integer.parseInt(countStr);
        } catch (NumberFormatException e) {
            throw new ProtocolException("Invalid array count: " + countStr);
        }

        List<RespValue> items = new ArrayList<>();
        int cursor = countLineEnd + 2;

        for (int i = 0; i < count; i++) {
            Optional<ParseResult> item = parseOne(cursor);
            if (item.isEmpty()) {
                // An inner element isn't fully buffered yet -- the ENTIRE
                // array is therefore incomplete. Return empty rather than
                // a partially-built array; consumedUpTo must not advance.
                return Optional.empty();
            }
            items.add(item.get().value());
            cursor = item.get().nextIndex();
        }

        return Optional.of(new ParseResult(new RespValue.Array(items), cursor));
    }

    // How many unconsumed bytes remain in the buffer right now.
    private int remainingLength() {
        return buffer.length - consumedUpTo;
    }

    /**
     * Drops already-consumed bytes from the front of the buffer so it
     * doesn't grow unboundedly across many feed()/tryDecode() cycles on
     * a long-lived connection. Called lazily, only from feed(), rather
     * than after every single parse -- trades a little extra memory for
     * fewer array copies.
     */
    private void compactIfNeeded() {
        if (consumedUpTo == 0) return;
        byte[] trimmed = new byte[remainingLength()];
        System.arraycopy(buffer, consumedUpTo, trimmed, 0, remainingLength());
        buffer = trimmed;
        consumedUpTo = 0;
    }

    /** Thrown when the buffer contains bytes that don't form valid RESP. */
    public static final class ProtocolException extends RuntimeException {
        public ProtocolException(String message) {
            super(message);
        }
    }
}