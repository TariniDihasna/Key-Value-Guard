package kvguard.store;

public class FixedLeader implements LeaderInfo{
    private final String selfId;
    private final String leaderId;

    public FixedLeader(String selfId, String leaderId) {
        this.selfId = selfId;
        this.leaderId = leaderId;
    }

    @Override
    public boolean isLeader(){
        return selfId.equals(leaderId);
    }

    @Override
    public String leaderId(){
        return leaderId;
    }
}
