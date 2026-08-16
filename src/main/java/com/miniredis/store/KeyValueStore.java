package com.miniredis.store;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * The heart of Mini Redis: holds all key-value data, enforces TTL expiry,
 * and evicts the least-recently-used key once a configured capacity is
 * exceeded.
 *
 * THREAD SAFETY NOTE: this class is deliberately NOT synchronized. That's
 * not an oversight -- the reactor server (coming later) is single-threaded
 * by design, meaning only one thread ever touches this store at a time.
 * Adding synchronization here would be dead weight: extra overhead for a
 * guarantee we already get for free from the architecture. If this store
 * were ever used from multiple threads, this assumption would need to be
 * revisited explicitly.
 */
public final class KeyValueStore {

   /**
     * The main data map, in ACCESS-ORDER mode. Passing `true` as the third
     * constructor argument is what makes LinkedHashMap reorder entries so
     * that the most recently get()'d or put()'d entry moves to the end of
     * iteration order, and the least recently used entry sits at the
     * front. This is the entire mechanism LRU eviction is built on.
     *
     * removeEldestEntry() is a hook LinkedHashMap calls after every put():
     * if it returns true, the eldest (least recently used) entry is
     * automatically removed. We use it to enforce maxKeys.
     *
     * DESIGN NOTE -- no separate EvictionPolicy abstraction: LinkedHashMap
     * already provides a correct, O(1), well-tested LRU implementation.
     * Extracting an EvictionPolicy interface here would add a layer of
     * indirection to hypothetically support a second algorithm (e.g. LFU)
     * that isn't a real requirement. If a second eviction strategy becomes
     * an actual need, that's the point at which this logic should be
     * pulled out behind an interface -- not before (YAGNI).
     */
    private final LinkedHashMap<String, StoredValue> data;

    /** key -> absolute expiry time in epoch milliseconds. No entry here means "no TTL set." */
    private final Map<String, Long> expiryTimestamps = new HashMap<>();

    private final int maxKeys;

    public KeyValueStore(int maxKeys) {
        this.maxKeys = maxKeys;
        // initialCapacity=16, loadFactor=0.75f are LinkedHashMap's normal
        // defaults -- we're not changing sizing behavior, only enabling
        // access-order mode (the `true` third argument).
        this.data = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, StoredValue> eldest) {
                boolean shouldEvict = size() > maxKeys;
                if (shouldEvict) {
                    // Keep the TTL map in sync -- if we don't remove the
                    // evicted key's expiry entry too, expiryTimestamps
                    // would silently leak entries for keys that no longer
                    // exist in `data`, growing forever on a long-lived
                    // server under eviction pressure.
                    expiryTimestamps.remove(eldest.getKey());
                }
                return shouldEvict;
            }
        };
    }

    /**
     * Retrieves a value, applying passive expiry first.
     * Returns Optional.empty() if the key doesn't exist OR has expired --
     * callers (commands) don't need to distinguish those two cases, both
     * mean "there is nothing here right now."
     */
    public Optional<StoredValue> get(String key) {
        removeIfExpired(key);
        // LinkedHashMap.get() in access-order mode has a side effect:
        // calling it moves this entry to the most-recently-used position.
        // That side effect IS the LRU mechanism -- every read counts as
        // a "use," exactly as the feature list requires.
        StoredValue value = data.get(key);
        return Optional.ofNullable(value);
    }

    /**
     * Stores a value under a key, overwriting any prior value and clearing
     * any prior TTL -- this matches real Redis semantics: a plain SET
     * without EX removes any expiry the key previously had.
     */
    public void set(String key, StoredValue value) {
        data.put(key, value);
        expiryTimestamps.remove(key);
    }

    /** Deletes a key. Returns true if it existed (and thus was actually removed). */
    public boolean del(String key) {
        removeIfExpired(key);
        boolean existed = data.remove(key) != null;
        expiryTimestamps.remove(key);
        return existed;
    }

    /** Checks existence, applying passive expiry first. */
    public boolean exists(String key) {
        removeIfExpired(key);
        return data.containsKey(key);
    }

    /**
     * Sets a TTL on an existing key, `seconds` from now.
     * Returns false (and does nothing) if the key doesn't currently exist --
     * matches real Redis: EXPIRE on a missing key is a no-op that reports
     * failure, not an error.
     */
    public boolean expire(String key, long seconds) {
        removeIfExpired(key);
        if (!data.containsKey(key)) {
            return false;
        }
        long expiryAt = System.currentTimeMillis() + (seconds * 1000);
        expiryTimestamps.put(key, expiryAt);
        return true;
    }

    /**
     * Returns remaining TTL in seconds, using real Redis's exact
     * conventions:
     *   -2  if the key does not exist (or has expired)
     *   -1  if the key exists but has no TTL set
     *   >=0 remaining seconds otherwise
     * Returning these as sentinel values (not an Optional/exception) is
     * deliberate -- it mirrors what real redis-cli expects on the wire
     * (a plain RESP Integer), so TtlCommand can pass this straight through
     * with no translation.
     */
    public long ttlSeconds(String key) {
        removeIfExpired(key);
        if (!data.containsKey(key)) {
            return -2;
        }
        Long expiryAt = expiryTimestamps.get(key);
        if (expiryAt == null) {
            return -1;
        }
        long remainingMillis = expiryAt - System.currentTimeMillis();
        // Round down to whole seconds; if this races to just-under-zero
        // between the expiry check above and here, clamp to 0 rather than
        // returning a negative "remaining" value, which would be nonsense.
        return Math.max(0, remainingMillis / 1000);
    }

    /**
     * Checks whether a key has an expiry that has passed, and if so,
     * removes it from BOTH maps. This is "passive expiry": we only ever
     * check a key's TTL when something touches that specific key, rather
     * than continuously scanning. Called at the START of every public
     * method above, before any LinkedHashMap access-order side effect
     * happens -- this ordering matters, see the class-level explanation.
     */
    private void removeIfExpired(String key) {
        Long expiryAt = expiryTimestamps.get(key);
        if (expiryAt != null && System.currentTimeMillis() >= expiryAt) {
            data.remove(key);
            expiryTimestamps.remove(key);
        }
    }

    /**
     * Exposes read-only visibility into which keys currently have a TTL
     * set, WITHOUT triggering any LRU access-order side effect (unlike
     * get()). This exists specifically for ExpiryEngine's active expiry
     * sampling (coming next) -- active expiry must be able to inspect
     * keys with TTLs without accidentally marking them as "recently
     * used" just by checking whether they've expired.
     */
    java.util.Set<String> keysWithTtl() {
        return expiryTimestamps.keySet();
    }

    /** Package-private, used by ExpiryEngine to actually expire a key during active sampling. */
    void forceExpire(String key) {
        data.remove(key);
        expiryTimestamps.remove(key);
    }

    /** Package-private, used by ExpiryEngine to check a specific key's expiry without side effects. */
    boolean isExpiredNow(String key) {
        Long expiryAt = expiryTimestamps.get(key);
        return expiryAt != null && System.currentTimeMillis() >= expiryAt;
    }

    public int size() {
        return data.size();
    }
}