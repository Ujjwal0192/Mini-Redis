package com.miniredis.command;

import com.miniredis.protocol.RespDecoder;
import com.miniredis.protocol.RespValue;
import com.miniredis.store.KeyValueStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The real milestone test: raw bytes in, exactly as they'd arrive off a
 * real socket, all the way through to a final RespValue response -- with
 * NO socket, NO reactor, involved at all. This proves RespDecoder and
 * CommandDispatcher compose correctly together, which is the entire
 * server's logic minus networking and disk persistence.
 *
 * If this test suite passes, the only thing left to build is plumbing:
 * wiring these exact same objects up to real socket I/O. The correctness
 * of the actual Redis behavior is already proven here.
 */
class EndToEndPipelineTest {

    private RespDecoder decoder;
    private CommandDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        decoder = new RespDecoder();
        KeyValueStore store = new KeyValueStore(100);
        dispatcher = new CommandDispatcher(store);
    }

    /**
     * Simulates exactly what the reactor will eventually do per socket
     * read: feed raw bytes to the decoder, pull out every complete
     * command, dispatch each one, and return the list of responses.
     * This helper IS effectively a preview of ReactorServer's core loop
     * logic, minus the actual socket.
     */
    private List<RespValue> processWireBytes(String wireFormat) {
        byte[] bytes = wireFormat.getBytes(StandardCharsets.UTF_8);
        decoder.feed(bytes, bytes.length);

        List<RespValue> responses = decoder.tryDecode().stream()
            .map(decoded -> dispatcher.dispatch((RespValue.Array) decoded))
            .toList();

        return responses;
    }

    @Test
    void setCommandOverTheWireReturnsOk() {
        // Exactly what redis-cli sends for: SET foo bar
        String wire = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";

        List<RespValue> responses = processWireBytes(wire);

        assertEquals(1, responses.size());
        assertEquals(new RespValue.SimpleString("OK"), responses.get(0));
    }

    @Test
    void setThenGetOverTheWireRoundTripsCorrectly() {
        processWireBytes("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");

        List<RespValue> responses =
            processWireBytes("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");

        assertEquals(1, responses.size());
        assertEquals(new RespValue.BulkString("bar"), responses.get(0));
    }

    @Test
    void pipelinedSetAndGetInOneReadBothExecuteInOrder() {
        // SET foo bar, immediately followed by GET foo, in ONE wire
        // payload -- exactly the pipelining scenario RespDecoder was
        // built to handle. This proves the dispatcher correctly
        // processes MULTIPLE decoded commands from a single feed().
        String wire = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
                    + "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";

        List<RespValue> responses = processWireBytes(wire);

        assertEquals(2, responses.size());
        assertEquals(new RespValue.SimpleString("OK"), responses.get(0));
        assertEquals(new RespValue.BulkString("bar"), responses.get(1));
    }

    @Test
    void getOnMissingKeyOverTheWireReturnsNullBulk() {
        List<RespValue> responses =
            processWireBytes("*2\r\n$3\r\nGET\r\n$7\r\nmissing\r\n");

        assertEquals(1, responses.size());
        assertInstanceOf(RespValue.NullBulk.class, responses.get(0));
    }

    @Test
    void incrOverTheWireIncrementsAndPersistsAcrossCalls() {
        processWireBytes("*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n");
        List<RespValue> secondCall =
            processWireBytes("*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n");

        assertEquals(new RespValue.Integer(2), secondCall.get(0));
    }

    @Test
    void unknownCommandOverTheWireReturnsError() {
        String wire = "*1\r\n$7\r\nNOTREAL\r\n";

        List<RespValue> responses = processWireBytes(wire);

        assertEquals(1, responses.size());
        assertInstanceOf(RespValue.Error.class, responses.get(0));
    }

    @Test
    void commandNameIsCaseInsensitiveOverTheWire() {
        // lowercase "set" -- real redis-cli and real Redis both accept
        // this; CommandDispatcher's toUpperCase(Locale.ROOT) normalization
        // is what this test actually exercises.
        String wire = "*3\r\n$3\r\nset\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";

        List<RespValue> responses = processWireBytes(wire);

        assertEquals(new RespValue.SimpleString("OK"), responses.get(0));
    }

    @Test
    void fullRealisticSessionSetGetDelExistsIncr() {
        // A believable sequence of commands in one "connection," proving
        // state correctly persists across dispatched commands via the
        // SAME store instance -- exactly how one real client session
        // would behave.
        assertEquals(new RespValue.SimpleString("OK"),
            processWireBytes("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n").get(0));

        assertEquals(new RespValue.Integer(1),
            processWireBytes("*2\r\n$6\r\nEXISTS\r\n$3\r\nfoo\r\n").get(0));

        assertEquals(new RespValue.Integer(1),
            processWireBytes("*2\r\n$3\r\nDEL\r\n$3\r\nfoo\r\n").get(0));

        assertEquals(new RespValue.Integer(0),
            processWireBytes("*2\r\n$6\r\nEXISTS\r\n$3\r\nfoo\r\n").get(0));

        assertEquals(new RespValue.Integer(1),
            processWireBytes("*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n").get(0));
    }
}