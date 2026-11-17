
package kvguard.clock;


public class LamportClock {

    private long time = 0;

    
    public synchronized long tick() {
        time++;
        return time;
    }

    
    public synchronized long onSend() {
        time++;
        return time;
    }

    
    public synchronized long onReceive(long receivedTimestamp) {
        time = Math.max(time, receivedTimestamp) + 1;
        return time;
    }

    
    public synchronized long getTime() {
        return time;
    }
}

