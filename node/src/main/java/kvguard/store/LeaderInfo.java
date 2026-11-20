package kvguard.store;

public interface LeaderInfo {
    boolean isLeader();

    String leaderId();
}
