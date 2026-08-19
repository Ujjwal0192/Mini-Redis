package com.miniredis.command;

import com.miniredis.command.list.LPopCommand;
import com.miniredis.command.list.LPushCommand;
import com.miniredis.command.list.LRangeCommand;
import com.miniredis.command.list.RPopCommand;
import com.miniredis.command.list.RPushCommand;
import com.miniredis.command.string.DelCommand;
import com.miniredis.command.string.ExistsCommand;
import com.miniredis.command.string.ExpireCommand;
import com.miniredis.command.string.GetCommand;
import com.miniredis.command.string.IncrCommand;
import com.miniredis.command.string.SetCommand;
import com.miniredis.command.string.TtlCommand;
import com.miniredis.protocol.RespValue;
import com.miniredis.store.KeyValueStore;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Routes a decoded command (a RespValue.Array of BulkStrings, e.g.
 * ["SET", "foo", "bar"]) to the correct Command implementation, and
 * returns whatever RespValue that command produces.
 *
 * This is intentionally the ONLY place in the codebase that knows the
 * mapping from command name string to Command class. Individual commands
 * (SetCommand, GetCommand, ...) have no idea what name they're registered
 * under -- that decoupling means renaming or aliasing a command later is
 * a one-line change here, not a change to the command's own class.
 */
public final class CommandDispatcher {

    // Command names are stored UPPERCASE as keys -- lookups normalize the
    // incoming name to uppercase too, which is what makes dispatch
    // case-insensitive ("set", "SET", "SeT" all resolve to the same entry).
    private final Map<String, Command> commands = new HashMap<>();

    private final KeyValueStore store;

    public CommandDispatcher(KeyValueStore store) {
        this.store = store;
        registerCommands();
    }

    /**
     * One-time setup: each command is instantiated exactly ONCE here and
     * reused for every single request from every client, for the entire
     * server's lifetime. This works because Command implementations are
     * stateless (see the concept notes on Command.java) -- there's no
     * per-request state living inside a SetCommand instance that would
     * make sharing it across requests unsafe.
     */
   private void registerCommands() {
        commands.put("SET", new SetCommand());
        commands.put("GET", new GetCommand());
        commands.put("DEL", new DelCommand());
        commands.put("EXISTS", new ExistsCommand());
        commands.put("INCR", new IncrCommand());
        commands.put("EXPIRE", new ExpireCommand());
        commands.put("TTL", new TtlCommand());
        commands.put("LPUSH", new LPushCommand());
        commands.put("RPUSH", new RPushCommand());
        commands.put("LPOP", new LPopCommand());
        commands.put("RPOP", new RPopCommand());
        commands.put("LRANGE", new LRangeCommand());
    }

    /**
     * Dispatches a single decoded command array to the right Command and
     * returns its response. This is the method the reactor (once built)
     * will call once per fully-decoded RespValue coming out of a client's
     * RespDecoder.
     *
     * @param commandArray must be a RespValue.Array whose first element is
     *                     the command name as a BulkString, and remaining
     *                     elements are its arguments -- exactly the shape
     *                     RESP clients send for every command.
     */
    public RespValue dispatch(RespValue.Array commandArray) {
        List<RespValue> items = commandArray.items();

        if (items.isEmpty()) {
            return new RespValue.Error("ERR empty command");
        }

        // The command name itself must be a BulkString -- this is what
        // real RESP clients always send. If it's some other RespValue
        // type, that's a malformed command, not a valid-but-unknown one.
        if (!(items.get(0) instanceof RespValue.BulkString nameValue)) {
            return new RespValue.Error("ERR invalid command format");
        }

        String commandName = nameValue.value().toUpperCase(Locale.ROOT);
        Command command = commands.get(commandName);

        if (command == null) {
            return new RespValue.Error("ERR unknown command '" + nameValue.value() + "'");
        }

        List<String> args = extractArgs(items);
        return command.execute(args, store);
    }

    /**
     * Exposes whether a given command name is mutating, WITHOUT exposing
     * the underlying Command instance or the internal name-to-Command
     * map itself. This is the one piece of dispatcher-internal
     * information ReactorServer legitimately needs (to decide whether to
     * log to AOF) without needing broader access to dispatch internals.
     */
    public boolean isMutatingCommand(String commandName) {
        Command command = commands.get(commandName.toUpperCase(Locale.ROOT));
        return command != null && command.isMutating();
    }

    /**
     * Converts every element AFTER the command name (index 0) from
     * RespValue.BulkString down to plain String -- this is the exact
     * point described in Command.java's concept notes where RESP wire
     * types get stripped away, leaving individual commands to deal only
     * in plain strings and never know RESP exists.
     */
    private List<String> extractArgs(List<RespValue> items) {
        return items.subList(1, items.size()).stream()
            .map(this::extractStringValue)
            .toList();
    }

    /**
     * Real RESP clients (including redis-cli) always send command
     * arguments as BulkStrings. If some other type shows up here, that's
     * a malformed request -- we fail loudly with a ProtocolException-style
     * runtime exception rather than silently coercing via toString(),
     * which could hide a real client bug behind seemingly-working output.
     */
    private String extractStringValue(RespValue value) {
        if (value instanceof RespValue.BulkString bulk) {
            return bulk.value();
        }
        throw new IllegalArgumentException(
            "Expected BulkString argument, got: " + value.getClass().getSimpleName());
    }
}