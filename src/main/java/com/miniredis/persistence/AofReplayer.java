package com.miniredis.persistence;

import com.miniredis.command.CommandDispatcher;
import com.miniredis.protocol.RespDecoder;
import com.miniredis.protocol.RespValue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Reads the AOF log file back on startup and re-executes every command it
 * contains, in the exact order they were originally written, against a
 * fresh KeyValueStore -- reconstructing whatever state existed right
 * before the server last stopped (cleanly or via crash, doesn't matter,
 * since every mutating command was durably fsync'd as it happened).
 *
 * This class does NOT care about the RespValue each command execution
 * returns -- during replay there's no client waiting for a response, we
 * only care about the SIDE EFFECT each command has on the store (a SET
 * actually storing a value, an LPUSH actually mutating a list, etc).
 */
public final class AofReplayer {

    private final Path aofPath;
    private final CommandDispatcher dispatcher;

    public AofReplayer(Path aofPath, CommandDispatcher dispatcher) {
        this.aofPath = aofPath;
        this.dispatcher = dispatcher;
    }

    /**
     * Replays the entire AOF file, if it exists. Called once, at startup,
     * BEFORE the reactor begins accepting client connections -- this
     * ordering matters: a client connecting before replay finished could
     * read stale/incomplete state, or worse, a client's fresh write could
     * get interleaved with replay and end up processed out of order.
     *
     * Returns the number of commands replayed, mainly useful for a
     * startup log message ("Replayed 4,213 commands from AOF").
     */
    public int replay() throws IOException {
        if (!Files.exists(aofPath)) {
            // First-ever run, or AOF was deliberately deleted -- not an
            // error, just means "nothing to replay, start empty."
            return 0;
        }

        byte[] fileBytes = Files.readAllBytes(aofPath);

        // Reuses the EXACT SAME RespDecoder class that handles live
        // socket bytes -- this isn't a coincidence, it's the entire
        // point of building the decoder as a general "bytes in, RespValue
        // out" component back when we first wrote it, rather than
        // something coupled to sockets specifically.
        RespDecoder decoder = new RespDecoder();
        decoder.feed(fileBytes, fileBytes.length);
        List<RespValue> commands = decoder.tryDecode();

        int replayedCount = 0;
        for (RespValue decoded : commands) {
            if (!(decoded instanceof RespValue.Array array)) {
                // A malformed or corrupted entry in the AOF file. We
                // choose to skip it and continue rather than aborting
                // the entire replay -- one bad entry (e.g. from a crash
                // that happened mid-write) shouldn't cost you every
                // OTHER valid command that came before and after it in
                // the log. This is a real design choice worth being able
                // to defend: "fail loudly and lose everything" vs "skip
                // the bad entry and recover as much as possible" -- we
                // chose the latter, prioritizing maximum state recovery.
                continue;
            }
            // dispatch()'s return value (the RespValue response) is
            // deliberately discarded here -- during replay we only care
            // about the mutation's side effect on the store, there's no
            // client connection to send a response to.
            dispatcher.dispatch(array);
            replayedCount++;
        }

        return replayedCount;
    }
}