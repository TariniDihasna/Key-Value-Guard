package kvguard.store;

import kvguard.proto.LogEntry;
import kvguard.store.KeyValueStore.ApplyResult;

public class LocalReplicator implements Replicator {
    private final KeyValueStore store;

    public LocalReplicator(KeyValueStore store){
        this.store = store;
    }

    @Override
    public ApplyResult replicate(LogEntry entry){
        return store.apply(entry);
    }

}
