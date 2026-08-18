package com.miniredis.command.string;

import com.miniredis.protocol.RespValue;
import com.miniredis.store.KeyValueStore;
import com.miniredis.store.StoredValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DelExistsIncrCommandTest {

    private KeyValueStore store;
    private SetCommand setCommand;
    private DelCommand delCommand;
    private ExistsCommand existsCommand;
    private IncrCommand incrCommand;

    @BeforeEach
    void setUp() {
        store = new KeyValueStore(100);
        setCommand = new SetCommand();
        delCommand = new DelCommand();
        existsCommand = new ExistsCommand();
        incrCommand = new IncrCommand();
    }

    // ---------- DEL ----------

    @Test
    void delOnExistingKeyReturnsOneAndRemovesIt() {
        setCommand.execute(List.of("foo", "bar"), store);

        RespValue result = delCommand.execute(List.of("foo"), store);

        assertEquals(new RespValue.Integer(1), result);
        assertFalse(store.exists("foo"));
    }

    @Test
    void delOnMissingKeyReturnsZero() {
        RespValue result = delCommand.execute(List.of("neverExisted"), store);
        assertEquals(new RespValue.Integer(0), result);
    }

    @Test
    void delWithMultipleKeysReturnsCountOfActuallyDeleted() {
        setCommand.execute(List.of("a", "1"), store);
        setCommand.execute(List.of("b", "2"), store);
        // "c" is deliberately never set -- tests that DEL counts only
        // keys that actually existed, not just how many were named.

        RespValue result = delCommand.execute(List.of("a", "b", "c"), store);

        assertEquals(new RespValue.Integer(2), result);
    }

    // ---------- EXISTS ----------

    @Test
    void existsOnPresentKeyReturnsOne() {
        setCommand.execute(List.of("foo", "bar"), store);
        RespValue result = existsCommand.execute(List.of("foo"), store);
        assertEquals(new RespValue.Integer(1), result);
    }

    @Test
    void existsOnMissingKeyReturnsZero() {
        RespValue result = existsCommand.execute(List.of("neverExisted"), store);
        assertEquals(new RespValue.Integer(0), result);
    }

    @Test
    void existsCountsTheSameKeyTwiceIfPassedTwice() {
        setCommand.execute(List.of("foo", "bar"), store);

        // EXISTS foo foo -- real Redis counts this as 2, not 1, since it
        // doesn't deduplicate the key list.
        RespValue result = existsCommand.execute(List.of("foo", "foo"), store);

        assertEquals(new RespValue.Integer(2), result);
    }

    // ---------- INCR ----------

    @Test
    void incrOnMissingKeyStartsAtOne() {
        RespValue result = incrCommand.execute(List.of("counter"), store);
        assertEquals(new RespValue.Integer(1), result);
    }

    @Test
    void incrOnExistingIntegerValueAddsOne() {
        setCommand.execute(List.of("counter", "41"), store);

        RespValue result = incrCommand.execute(List.of("counter"), store);

        assertEquals(new RespValue.Integer(42), result);
    }

    @Test
    void incrTwiceAccumulatesCorrectly() {
        incrCommand.execute(List.of("counter"), store);
        RespValue result = incrCommand.execute(List.of("counter"), store);

        assertEquals(new RespValue.Integer(2), result);
    }

    @Test
    void incrOnNonIntegerValueReturnsError() {
        setCommand.execute(List.of("counter", "notANumber"), store);

        RespValue result = incrCommand.execute(List.of("counter"), store);

        assertInstanceOf(RespValue.Error.class, result);
    }

    @Test
    void incrOnListValueReturnsWrongTypeError() {
        // Directly inject a ListValue -- we don't have LPUSH yet, so we
        // construct the StoredValue by hand to simulate "key holds a list."
        store.set("mylist", new StoredValue.ListValue(new ArrayDeque<>(List.of("a", "b"))));

        RespValue result = incrCommand.execute(List.of("mylist"), store);

        assertInstanceOf(RespValue.Error.class, result);
        assertTrue(((RespValue.Error) result).message().startsWith("WRONGTYPE"));
    }

    @Test
    void incrAtMaxLongValueReturnsOverflowError() {
        setCommand.execute(List.of("counter", String.valueOf(Long.MAX_VALUE)), store);

        RespValue result = incrCommand.execute(List.of("counter"), store);

        assertInstanceOf(RespValue.Error.class, result);
    }
}