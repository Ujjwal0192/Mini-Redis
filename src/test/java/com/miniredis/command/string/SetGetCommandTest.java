package com.miniredis.command.string;

import com.miniredis.protocol.RespValue;
import com.miniredis.store.KeyValueStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests SetCommand and GetCommand directly against a real KeyValueStore --
 * no socket, no decoder, no dispatcher. This isolates "does the command
 * logic itself behave correctly" from "does the plumbing around it work,"
 * which is exactly the point of unit testing each layer independently
 * before wiring layers together.
 */
class SetGetCommandTest {

    private KeyValueStore store;
    private SetCommand setCommand;
    private GetCommand getCommand;

    /**
     * Runs before EVERY @Test method, giving each test a fresh store --
     * critical here, since tests would otherwise leak state into each
     * other (e.g. a key set in one test still existing in the next),
     * making failures hard to reason about and order-dependent.
     */
    @BeforeEach
    void setUp() {
        store = new KeyValueStore(100); // maxKeys=100, plenty for these tests
        setCommand = new SetCommand();
        getCommand = new GetCommand();
    }

    @Test
    void setThenGetReturnsTheStoredValue() {
        RespValue setResult = setCommand.execute(List.of("foo", "bar"), store);
        assertEquals(new RespValue.SimpleString("OK"), setResult);

        RespValue getResult = getCommand.execute(List.of("foo"), store);
        assertEquals(new RespValue.BulkString("bar"), getResult);
    }

    @Test
    void getOnMissingKeyReturnsNullBulk() {
        RespValue getResult = getCommand.execute(List.of("doesNotExist"), store);
        assertInstanceOf(RespValue.NullBulk.class, getResult);
    }

    @Test
    void setOverwritesExistingValue() {
        setCommand.execute(List.of("foo", "first"), store);
        setCommand.execute(List.of("foo", "second"), store);

        RespValue getResult = getCommand.execute(List.of("foo"), store);
        assertEquals(new RespValue.BulkString("second"), getResult);
    }

    @Test
    void setWithExSetsATtlThatGetDoesNotBypass() {
        // SET foo bar EX 100 -- should be readable immediately, TTL
        // shouldn't affect a fresh read.
        setCommand.execute(List.of("foo", "bar", "EX", "100"), store);

        RespValue getResult = getCommand.execute(List.of("foo"), store);
        assertEquals(new RespValue.BulkString("bar"), getResult);

        // Confirm the TTL was actually recorded via the store directly.
        long ttl = store.ttlSeconds("foo");
        assertTrue(ttl > 0 && ttl <= 100, "TTL should be set and roughly 100 seconds");
    }

    @Test
    void plainSetClearsAnyPreviousTtl() {
        setCommand.execute(List.of("foo", "bar", "EX", "100"), store);
        assertTrue(store.ttlSeconds("foo") > 0);

        // Overwriting with a plain SET (no EX) should remove the TTL --
        // this directly tests the real-Redis semantic SetCommand relies
        // on KeyValueStore.set() to provide.
        setCommand.execute(List.of("foo", "newValue"), store);

        assertEquals(-1, store.ttlSeconds("foo"), "plain SET should clear prior TTL");
    }

    @Test
    void setWithTooFewArgumentsReturnsError() {
        RespValue result = setCommand.execute(List.of("onlyOneArg"), store);
        assertInstanceOf(RespValue.Error.class, result);
    }

    @Test
    void getWithWrongArgumentCountReturnsError() {
        RespValue result = getCommand.execute(List.of("too", "many", "args"), store);
        assertInstanceOf(RespValue.Error.class, result);
    }

    @Test
    void setWithInvalidExSecondsReturnsError() {
        RespValue result = setCommand.execute(
            List.of("foo", "bar", "EX", "notANumber"), store);
        assertInstanceOf(RespValue.Error.class, result);
    }
}