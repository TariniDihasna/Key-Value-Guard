package kvguard.store;

public class ReplicationException extends RuntimeException{
    public enum Reason {NOT_LEADER, NO_QUORUM }
    private final Reason reason;
    private final String leaderHint;

    public ReplicationException(Reason reason,String leaderHint,String message){
        super(message);
        this.reason = reason;
        this.leaderHint = leaderHint == null ? "" : leaderHint;
    }

    public Reason reason() {return reason;}

    public String leaderHint() {return leaderHint;}
}
