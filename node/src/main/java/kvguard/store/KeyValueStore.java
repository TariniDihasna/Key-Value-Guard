package kvguard.store;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * ROLE 4 (Replication and consistency) starting point.
 *
 * A very small in-memory key-value store with versions, compare-and-set (CAS) and
 * duplicate-request protection. Right now every request is applied directly on this one node.
 * Later, the store should change ONLY when ConsensusModule reports that a log entry is
 * committed (see docs/DESIGN.md, section 5). When that happens, "nextVersion" is replaced by
 * the real log index of the entry.
 */
public class KeyValueStore {

    public record Entry(String value, long version) { }

    public enum Result { OK, VERSION_MISMATCH, KEY_NOT_FOUND }

    public record ApplyResult(Result result, long version, boolean duplicate) { }

    private final Map<String, Entry> data = new HashMap<>();
    // Remembers what happened to each request id, so a retried request is not applied twice.
    private final Map<String, ApplyResult> applied = new HashMap<>();
    // Stand-in for the log index until consensus exists.
    private long nextVersion = 1;

    /** PUT: set the value, always allowed. */
    public synchronized ApplyResult put(String requestId, String key, String value) {
        ApplyResult earlier = lookup(requestId);
        if (earlier != null) {
            return earlier;
        }
        long version = nextVersion++;
        data.put(key, new Entry(value, version));
        return remember(requestId, new ApplyResult(Result.OK, version, false));
    }

    /** DELETE: remove the key, or KEY_NOT_FOUND if it does not exist. */
    public synchronized ApplyResult delete(String requestId, String key) {
        ApplyResult earlier = lookup(requestId);
        if (earlier != null) {
            return earlier;
        }
        if (!data.containsKey(key)) {
            return remember(requestId, new ApplyResult(Result.KEY_NOT_FOUND, 0, false));
        }
        data.remove(key);
        long version = nextVersion++;
        return remember(requestId, new ApplyResult(Result.OK, version, false));
    }

    /**
     * CAS: write only if the key's current version equals expectedVersion.
     * expectedVersion 0 means "the key must not exist yet" (for example, booking a free seat).
     */
    public synchronized ApplyResult cas(String requestId, String key, String value, long expectedVersion) {
        ApplyResult earlier = lookup(requestId);
        if (earlier != null) {
            return earlier;
        }
        Entry current = data.get(key);
        long currentVersion = (current == null) ? 0 : current.version();
        if (currentVersion != expectedVersion) {
            return remember(requestId, new ApplyResult(Result.VERSION_MISMATCH, currentVersion, false));
        }
        long version = nextVersion++;
        data.put(key, new Entry(value, version));
        return remember(requestId, new ApplyResult(Result.OK, version, false));
    }

    public synchronized Optional<Entry> get(String key) {
        return Optional.ofNullable(data.get(key));
    }

    /** How far this node has applied changes (later: the last applied log index). */
    public synchronized long lastApplied() {
        return nextVersion - 1;
    }

    private ApplyResult lookup(String requestId) {
        if (requestId == null || requestId.isEmpty()) {
            return null;
        }
        ApplyResult earlier = applied.get(requestId);
        if (earlier == null) {
            return null;
        }
        return new ApplyResult(earlier.result(), earlier.version(), true);
    }

    private ApplyResult remember(String requestId, ApplyResult result) {
        if (requestId != null && !requestId.isEmpty()) {
            applied.put(requestId, result);
        }
        return result;
    }
}
