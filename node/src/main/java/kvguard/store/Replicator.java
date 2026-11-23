package kvguard.store;

import kvguard.proto.LogEntry;
import kvguard.store.KeyValueStore.ApplyResult;

public interface Replicator {
    ApplyResult replicate(LogEntry entry);
}
