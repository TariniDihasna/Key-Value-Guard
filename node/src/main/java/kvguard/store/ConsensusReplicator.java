package kvguard.store;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import kvguard.consensus.ConsensusModule;
import kvguard.consensus.ConsensusModule.NoQuorumException;
import kvguard.consensus.ConsensusModule.NotLeaderException;
import kvguard.proto.LogEntry;
import kvguard.store.KeyValueStore.ApplyResult;

public class ConsensusReplicator implements Replicator, LeaderInfo{
    private final KeyValueStore store;
    private ConsensusModule consensus;
    private final Map<String,ApplyResult> waiting = new ConcurrentHashMap<>();
    private static final ApplyResult PENDING = new ApplyResult(null,0, false);

    public ConsensusReplicator(KeyValueStore store){
        this.store = store;
    }

    public void attach(ConsensusModule consensus) {
        this.consensus = consensus;
    }

    public void onCommit(LogEntry entry){
        ApplyResult result = store.apply(entry);
        if(waiting.containsKey(entry.getRequestId())){
            waiting.put(entry.getRequestId(), result);
        }
    }

    @Override
    public ApplyResult replicate(LogEntry entry){
        String id = entry.getRequestId();
        waiting.put(id,PENDING);
        try {
            consensus.propose(entry);
            ApplyResult result = waiting.get(id);
            if(result == null || result == PENDING) {
                throw new ReplicationException(ReplicationException.Reason.NO_QUORUM,"","entry not applied");
            }
            return result;
        }catch (NotLeaderException e){
            throw new ReplicationException(ReplicationException.Reason.NOT_LEADER,e.leaderId(),e.getMessage());
        }catch (NoQuorumException e){
            throw new ReplicationException(ReplicationException.Reason.NO_QUORUM,"",e.getMessage());
        }finally {
            waiting.remove(id);
        }
    }

    @Override
    public boolean isLeader() {return consensus.isLeader();}

    @Override
    public String leaderId() {return consensus.leaderId();}

    @Override
    public boolean confirmLeadership() {return consensus.confirmLeadership();}
}
