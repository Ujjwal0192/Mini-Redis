package com.miniredis.persistence;

import com.miniredis.command.CommandDispatcher;
import com.miniredis.store.KeyValueStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class AofPersistenceTest {

    @TempDir
    Path tempDir;

    @Test
    void writingThenReplayingReconstructsSimpleState() throws IOException {
        Path aofPath = tempDir.resolve("test.aof");
        AofWriter writer = new AofWriter(aofPath);

        writer.append("SET", List.of("foo", "bar"));
        writer.append("SET", List.of("counter", "1"));

        KeyValueStore freshStore = new KeyValueStore(100);
        CommandDispatcher freshDispatcher = new CommandDispatcher(freshStore);
        AofReplayer replayer = new AofReplayer(aofPath, freshDispatcher);

        int replayedCount = replayer.replay();

        assertEquals(2, replayedCount);
        assertEquals("bar", freshStore.get("foo").map(v ->
            ((com.miniredis.store.StoredValue.StringValue) v).value()).orElse(null));
        assertEquals("1", freshStore.get("counter").map(v ->
            ((com.miniredis.store.StoredValue.StringValue) v).value()).orElse(null));
    }

    @Test
    void replayOnMissingFileReturnsZeroAndDoesNotThrow() throws IOException {
        Path nonExistentPath = tempDir.resolve("never-written.aof");

        KeyValueStore store = new KeyValueStore(100);
        CommandDispatcher dispatcher = new CommandDispatcher(store);
        AofReplayer replayer = new AofReplayer(nonExistentPath, dispatcher);

        int replayedCount = replayer.replay();

        assertEquals(0, replayedCount, "missing AOF file should replay as zero commands, not throw");
    }

    @Test
    void replayPreservesExactOrderOfMutations() throws IOException {
        Path aofPath = tempDir.resolve("order.aof");
        AofWriter writer = new AofWriter(aofPath);

        writer.append("INCR", List.of("counter"));
        writer.append("INCR", List.of("counter"));
        writer.append("INCR", List.of("counter"));

        KeyValueStore freshStore = new KeyValueStore(100);
        CommandDispatcher freshDispatcher = new CommandDispatcher(freshStore);
        AofReplayer replayer = new AofReplayer(aofPath, freshDispatcher);
        replayer.replay();

        String finalValue = freshStore.get("counter")
            .map(v -> ((com.miniredis.store.StoredValue.StringValue) v).value())
            .orElse(null);

        assertEquals("3", finalValue, "three replayed INCRs should accumulate to exactly 3");
    }

    @Test
    void replayReconstructsListState() throws IOException {
        Path aofPath = tempDir.resolve("list.aof");
        AofWriter writer = new AofWriter(aofPath);

        writer.append("RPUSH", List.of("mylist", "a", "b"));
        writer.append("RPUSH", List.of("mylist", "c"));
        writer.append("LPOP", List.of("mylist"));

        KeyValueStore freshStore = new KeyValueStore(100);
        CommandDispatcher freshDispatcher = new CommandDispatcher(freshStore);
        AofReplayer replayer = new AofReplayer(aofPath, freshDispatcher);
        replayer.replay();

        var stored = freshStore.get("mylist");
        assertTrue(stored.isPresent());
        var listValue = (com.miniredis.store.StoredValue.ListValue) stored.get();
        assertEquals(List.of("b", "c"), List.copyOf(listValue.value()));
    }

    @Test
    void appendedCommandsAreValidRespWireFormat() throws IOException {
        Path aofPath = tempDir.resolve("format.aof");
        AofWriter writer = new AofWriter(aofPath);

        writer.append("SET", List.of("foo", "bar"));

        byte[] fileBytes = Files.readAllBytes(aofPath);
        String content = new String(fileBytes, java.nio.charset.StandardCharsets.UTF_8);

        assertEquals("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n", content);
    }

    @Test
    void multipleAppendsAccumulateInFileRatherThanOverwriting() throws IOException {
        Path aofPath = tempDir.resolve("accumulate.aof");
        AofWriter writer = new AofWriter(aofPath);

        writer.append("SET", List.of("a", "1"));
        writer.append("SET", List.of("b", "2"));

        byte[] fileBytes = Files.readAllBytes(aofPath);
        String content = new String(fileBytes, java.nio.charset.StandardCharsets.UTF_8);

        String expected = "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n"
                         + "*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n";
        assertEquals(expected, content);
    }
}