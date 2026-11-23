package kvguard.failure;

//Check and maintain the health status of the peer node
public class peerHealth {
    public enum State{
        UNKNOWN,
        ALIVE,
        SUSPECTED
    }
    private final String peerId;
    private State state = State.UNKNOWN;
    private int consecutiveFailures = 0;
    private long lastSuccessFullHeartbeatsMs = 0;

    public peerHealth(String peerId){
            this.peerId = peerId;
    }

    public synchronized boolean recordSuccess(long timestamp){
        boolean recovered = (state == State.SUSPECTED);
        state = State.ALIVE;
        consecutiveFailures = 0;
        lastSuccessFullHeartbeatsMs = timestamp;

        return recovered;
    }

    public synchronized boolean recordFailure(int failureThreshold){
        consecutiveFailures++;
        if(consecutiveFailures >= failureThreshold && state != State.SUSPECTED ){
            state = State.SUSPECTED;
            return true;
        }
        return false;
    }

    public String getPeerID(){
        return peerId;
    }

    public synchronized State getState(){
        return state;
    }
    public synchronized int getConsecutiveFailures(){
        return consecutiveFailures;
    }
    public synchronized long getLastSuccessfulHeartbeatMs(){
        return lastSuccessFullHeartbeatsMs;
    }
}
