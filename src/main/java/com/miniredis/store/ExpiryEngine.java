package com.miniredis.store;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Periodic active-expiry sampler, modeled loosely on real Redis's approach:
 * rather than scanning every key with a TTL on every cycle (expensive if
 * there are thousands), sample a bounded number of them at random, expire
 * whichever have passed their TTL, and repeat next cycle.
 *
 * This class does NOT own a thread or a timer. It exposes a single method,
 * runCycle(), that the reactor calls periodically from its own event loop
 * (using the selector's timeout, which we'll wire up in ReactorServer).
 * This keeps active expiry fully cooperative with the single-threaded
 * reactor model -- no synchronization needed anywhere, because this class
 * is only ever called from the one thread that also handles all socket I/O.
 */
public final class ExpiryEngine {

    private final KeyValueStore store;
    private final Random random = new Random();

    // How many keys-with-TTL to sample per cycle. Real Redis samples 20 by
    // default; we use the same figure -- small enough to be cheap even if
    // called frequently, large enough to make meaningful progress against
    // a large set of expiring keys.
    private static final int SAMPLE_SIZE = 20;

    public ExpiryEngine(KeyValueStore store) {
        this.store = store;
    }

    /**
     * Runs one sampling cycle: inspect up to SAMPLE_SIZE keys that have a
     * TTL set, expire any that have passed, and return how many were
     * actually expired this cycle. The reactor calls this on a fixed
     * interval (e.g., every 100ms) as part of its own loop.
     */
    public int runCycle() {
        Set<String> keysWithTtl = store.keysWithTtl();
        if (keysWithTtl.isEmpty()) {
            return 0;
        }

        List<String> sample = sampleKeys(keysWithTtl, SAMPLE_SIZE);

        int expiredCount = 0;
        for (String key : sample) {
            // isExpiredNow/forceExpire are package-private on KeyValueStore
            // -- ExpiryEngine lives in the same package (com.miniredis.store)
            // specifically so it can call these without exposing them as
            // part of the store's public API to commands or the reactor.
            if (store.isExpiredNow(key)) {
                store.forceExpire(key);
                expiredCount++;
            }
        }
        return expiredCount;
    }

    /**
     * Picks up to `count` random keys from the given set, without building
     * a full shuffled copy of a potentially large set every cycle.
     *
     * We convert to a List first because Set has no indexed access --
     * you can't do set.get(randomIndex), only iterate it. Once it's a
     * List, we can pick random indices directly.
     */
    private List<String> sampleKeys(Set<String> keys, int count) {
        List<String> asList = new ArrayList<>(keys);
        if (asList.size() <= count) {
            // Fewer keys-with-TTL exist than our sample size -- just
            // check all of them, no need to actually sample.
            return asList;
        }

        List<String> sample = new ArrayList<>(count);
        // Simple random sampling WITHOUT removal from asList and without
        // guaranteeing no duplicate picks within one cycle. A duplicate
        // pick just means we check the same key twice in one cycle, which
        // is harmless (isExpiredNow is a pure check, forceExpire is
        // idempotent) -- not worth the extra complexity of a proper
        // reservoir sample or shuffle for a 20-key sample.
        for (int i = 0; i < count; i++) {
            int index = random.nextInt(asList.size());
            sample.add(asList.get(index));
        }
        return sample;
    }
}