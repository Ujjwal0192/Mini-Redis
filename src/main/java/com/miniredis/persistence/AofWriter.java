package com.miniredis.persistence;

import com.miniredis.protocol.RespEncoder;
import com.miniredis.protocol.RespValue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * Appends every mutating command to a log file on disk, in RESP wire
 * format -- literally the same bytes a client would have sent to produce
 * that command. This is what makes state durable across a restart: on
 * startup, AofReplayer reads this file back and re-executes every command
 * exactly as if a client had sent it live.
 *
 * DURABILITY POLICY: fsync (force a physical disk write) after every
 * single append. This is the safest, simplest-to-reason-about option --
 * real Redis offers configurable policies here (always/everysec/no) as an
 * explicit durability-vs-throughput trade-off; we chose "always" for
 * correctness-by-default, at the cost of a real disk I/O per mutating
 * command. Under heavy write load this would meaningfully limit
 * throughput -- a legitimate, known limitation worth being able to state
 * plainly rather than something accidentally overlooked.
 */
public final class AofWriter {

    private final Path aofPath;

    public AofWriter(Path aofPath) {
        this.aofPath = aofPath;
    }

    /**
     * Appends one command to the log, encoded as a RESP array of bulk
     * strings -- the exact shape RespDecoder expects to read back later.
     *
     * @param commandName e.g. "SET" -- written uppercase for consistency,
     *                    though CommandDispatcher would accept any case
     *                    on replay anyway since dispatch is
     *                    case-insensitive.
     * @param args        the command's arguments, same shape Command.execute()
     *                    receives -- e.g. ["foo", "bar"] for SET foo bar.
     */
    public void append(String commandName, List<String> args) throws IOException {
        RespValue.Array commandArray = buildCommandArray(commandName, args);
        byte[] encoded = RespEncoder.encode(commandArray);

        // CREATE: make the file if it doesn't exist yet.
        // APPEND: always write at the end, never overwrite existing
        // content -- critical, since this file IS the durable history;
        // opening in a mode that could truncate or overwrite it would
        // destroy prior state.
        // WRITE: obviously, we're writing.
        Files.write(aofPath, encoded,
            StandardOpenOption.CREATE,
            StandardOpenOption.APPEND,
            StandardOpenOption.WRITE);

        // Files.write() above does NOT guarantee the bytes have hit
        // physical disk -- the OS may still be holding them in a buffer.
        // Explicitly forcing an fsync-equivalent here is what makes the
        // "fsync after every write" durability policy actually true,
        // not just something the comment claims.
        forceSyncToDisk();
    }

    /**
     * Builds "SET foo bar" (as an example) into the RespValue.Array shape
     * ["SET", "foo", "bar"] -- each element a BulkString, matching
     * exactly what a real client sends over the wire, and exactly what
     * CommandDispatcher.dispatch() expects to receive.
     */
    private RespValue.Array buildCommandArray(String commandName, List<String> args) {
        List<RespValue> items = new ArrayList<>();
        items.add(new RespValue.BulkString(commandName));
        for (String arg : args) {
            items.add(new RespValue.BulkString(arg));
        }
        return new RespValue.Array(items);
    }

    /**
     * Forces any OS-buffered writes to this file to be physically flushed
     * to disk. Opening a FileChannel specifically to call force() is a
     * bit heavier than ideal (we're opening the file a second time, right
     * after Files.write already opened and closed it once) -- a more
     * optimized version would keep one FileChannel open for the AOF
     * writer's whole lifetime rather than reopening per append. We're
     * choosing the simpler, more obviously correct version here; this is
     * a legitimate, known optimization opportunity, not an oversight.
     */
    private void forceSyncToDisk() throws IOException {
        try (var channel = java.nio.channels.FileChannel.open(aofPath, StandardOpenOption.WRITE)) {
            channel.force(true); // true = also force metadata (file size, etc), not just content
        }
    }
}