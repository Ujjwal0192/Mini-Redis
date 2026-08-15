package com.miniredis.protocol;

import org.junit.jupiter.api.Test;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for RespDecoder targeting the two hard problems it exists to solve:
 * partial frames (a command split across multiple socket reads) and
 * pipelined frames (multiple commands arriving in a single read).
 *
 * A decoder that only handles "one clean command, one clean read" isn't
 * actually correct -- it just hasn't been tested against real TCP behavior
 * yet, where reads can be chopped up arbitrarily by the OS/network.
 */
class RespDecoderTest {

    /** Helper: convert a wire-format string into raw bytes for feed(). */
    private byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void decodesACompleteSimpleCommandInOneShot() {
        RespDecoder decoder = new RespDecoder();
        // *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n  == SET foo bar
        byte[] wire = bytes("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");

        decoder.feed(wire, wire.length);
        List<RespValue> results = decoder.tryDecode();

        assertEquals(1, results.size(), "should decode exactly one command");
        RespValue.Array array = (RespValue.Array) results.get(0);
        assertEquals(3, array.items().size());
        assertEquals("SET", ((RespValue.BulkString) array.items().get(0)).value());
        assertEquals("foo", ((RespValue.BulkString) array.items().get(1)).value());
        assertEquals("bar", ((RespValue.BulkString) array.items().get(2)).value());
    }

    @Test
    void returnsEmptyWhenBufferHasNoCompleteFrameYet() {
        RespDecoder decoder = new RespDecoder();
        // Only the array header + first element's type/length -- nothing
        // near a complete command.
        byte[] partial = bytes("*3\r\n$3\r\nSE");

        decoder.feed(partial, partial.length);
        List<RespValue> results = decoder.tryDecode();

        assertTrue(results.isEmpty(), "must not produce a value from an incomplete frame");
    }

    /**
     * THE critical test: feeds the exact same command from the first test,
     * but one byte at a time, simulating the worst-case TCP scenario where
     * the OS delivers your data in tiny fragments. If any off-by-one exists
     * in findCrlf, parseBulkString's length math, or parseArray's atomicity,
     * this test will catch it -- a single-shot test cannot.
     */
    @Test
    void decodesACommandFedOneByteAtATime() {
        RespDecoder decoder = new RespDecoder();
        byte[] wire = bytes("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");

        RespValue.Array result = null;
        for (byte b : wire) {
            decoder.feed(new byte[]{b}, 1);
            List<RespValue> partialResults = decoder.tryDecode();
            // Every call except possibly the very last should return empty --
            // we only expect a result once the FINAL byte has been fed.
            if (!partialResults.isEmpty()) {
                assertEquals(1, partialResults.size());
                result = (RespValue.Array) partialResults.get(0);
            }
        }

        assertNotNull(result, "command should have been fully decoded by the last byte");
        assertEquals("SET", ((RespValue.BulkString) result.items().get(0)).value());
        assertEquals("foo", ((RespValue.BulkString) result.items().get(1)).value());
        assertEquals("bar", ((RespValue.BulkString) result.items().get(2)).value());
    }

    /**
     * The other critical test: two full commands arrive in a SINGLE feed()
     * call, exactly as would happen if a client pipelines commands and the
     * OS happens to deliver them together. tryDecode() must return BOTH,
     * not just the first -- silently dropping the second is a real bug
     * class, not a hypothetical.
     */
    @Test
    void decodesTwoPipelinedCommandsInOneRead() {
        RespDecoder decoder = new RespDecoder();
        // SET foo bar, followed immediately by GET foo
        String wire = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
                    + "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
        byte[] combined = bytes(wire);

        decoder.feed(combined, combined.length);
        List<RespValue> results = decoder.tryDecode();

        assertEquals(2, results.size(), "both pipelined commands must be decoded");

        RespValue.Array first = (RespValue.Array) results.get(0);
        assertEquals("SET", ((RespValue.BulkString) first.items().get(0)).value());

        RespValue.Array second = (RespValue.Array) results.get(1);
        assertEquals("GET", ((RespValue.BulkString) second.items().get(0)).value());
        assertEquals("foo", ((RespValue.BulkString) second.items().get(1)).value());
    }

    /**
     * Pipelining PLUS a partial trailing frame: two complete commands and
     * the START of a third, all in one feed(). Must decode exactly the two
     * complete ones and leave the partial third sitting in the buffer
     * untouched, ready for the next feed() call.
     */
    @Test
    void decodesCompleteCommandsAndLeavesPartialTrailingCommandBuffered() {
        RespDecoder decoder = new RespDecoder();
        String wire = "*1\r\n$4\r\nPING\r\n"           // complete: PING
                    + "*1\r\n$4\r\nPING\r\n"           // complete: PING
                    + "*2\r\n$3\r\nGET\r\n$3\r\nfo";    // incomplete: GET's value cut off mid-way
        byte[] combined = bytes(wire);

        decoder.feed(combined, combined.length);
        List<RespValue> results = decoder.tryDecode();

        assertEquals(2, results.size(), "only the two complete commands should decode");

        // Now feed the rest of the third command and confirm it completes.
        byte[] rest = bytes("o\r\n");
        decoder.feed(rest, rest.length);
        List<RespValue> finalResults = decoder.tryDecode();

        assertEquals(1, finalResults.size(), "the previously-partial command should now complete");
        RespValue.Array third = (RespValue.Array) finalResults.get(0);
        assertEquals("foo", ((RespValue.BulkString) third.items().get(1)).value());
    }

    @Test
    void decodesNullBulkString() {
        RespDecoder decoder = new RespDecoder();
        byte[] wire = bytes("$-1\r\n");

        decoder.feed(wire, wire.length);
        List<RespValue> results = decoder.tryDecode();

        assertEquals(1, results.size());
        assertInstanceOf(RespValue.NullBulk.class, results.get(0));
    }

    @Test
    void throwsProtocolExceptionOnUnknownTypeByte() {
        RespDecoder decoder = new RespDecoder();
        byte[] wire = bytes("!invalid\r\n"); // '!' is not a valid RESP type byte

        decoder.feed(wire, wire.length);

        assertThrows(RespDecoder.ProtocolException.class, decoder::tryDecode);
    }
}