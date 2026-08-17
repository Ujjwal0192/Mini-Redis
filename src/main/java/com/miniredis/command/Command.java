package com.miniredis.command;

import com.miniredis.protocol.RespValue;
import com.miniredis.store.KeyValueStore;

import java.util.List;

/**
 * A single Redis command (SET, GET, DEL, LPUSH, etc). Each concrete
 * command is its own small class implementing this one method -- this is
 * the Command design pattern: instead of one giant switch statement
 * handling every command inline, each command is independently testable,
 * independently readable, and adding a new command means adding a new
 * class, not editing a growing central method.
 *
 * This is a functional interface (exactly one abstract method), which
 * matters for CommandDispatcher: it means individual commands CAN be
 * written as lambdas if they're trivial, though we'll mostly use full
 * classes here since most commands need multiple lines of real logic
 * and benefit from a proper class name showing up in stack traces.
 */
@FunctionalInterface
public interface Command {

    /**
     * Executes this command against the store.
     *
     * @param args  the command's arguments, NOT including the command name
     *              itself -- for "SET foo bar", args is ["foo", "bar"].
     *              CommandDispatcher strips the command name before
     *              calling this, so every Command implementation only
     *              ever deals with its own arguments, never needs to
     *              know or check its own name.
     * @param store the shared KeyValueStore this command operates on.
     * @return the RespValue to send back to the client -- could be a
     *         SimpleString("OK"), a BulkString with a value, an Error if
     *         args are invalid, etc. Every command must return SOMETHING;
     *         there's no void/fire-and-forget path, since RESP always
     *         expects exactly one response per command.
     */
    RespValue execute(List<String> args, KeyValueStore store);

    /**
     * Whether this command mutates the store (and therefore needs to be
     * appended to the AOF log for durability). Defaults to true --
     * MUTATING is the more common and more consequential case to get
     * right, so new commands are safe-by-default: if someone adds a new
     * command and forgets to think about this method entirely, it's
     * logged (slightly wasteful for a read command) rather than silently
     * NOT logged (which would be a real data-loss bug for a write
     * command). Read-only commands explicitly override this to false.
     *
     * This is a default method -- interfaces can provide a body for a
     * method, and implementing classes only need to override it if they
     * want different behavior. This is exactly what SetCommand,
     * LPushCommand, etc. rely on implicitly (they don't override this at
     * all, so they inherit `true`), while GetCommand, ExistsCommand, etc.
     * explicitly override it to `false`.
     */
    default boolean isMutating() {
        return true;
    }
}