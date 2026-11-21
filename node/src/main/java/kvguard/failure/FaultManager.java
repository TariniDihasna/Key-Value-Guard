package kvguard.failure;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import kvguard.proto.FaultConfig;

public class FaultManager {
    private volatile int delayMs = 0;
    private volatile double dropProbability = 0.0;
    private volatile long clockSkewMs = 0;

    private final Set<String> blockedPeers = 
                ConcurrentHashMap.newKeySet();

    public synchronized String configure(FaultConfig config){
        if(config.getClearAll()){
            clearAll();
            return "All faults cleared successfullly";
        }
        if(config.getDelayMs() < 0){
            throw new IllegalArgumentException(
                "Delay cannot be negative"
            );
        }

        double probability = config.getDropProbability();

       if(!Double.isFinite(probability)
              || probability<0.0
              || probability > 1.0){
                    throw new IllegalArgumentException(
                        "Drop probability must be between 0.0 and 1.0"
                    );
              } 
    
              delayMs = config.getDelayMs();
              dropProbability = probability;
              clockSkewMs = config.getClockSkewMs();

        return "Fault configuration applied successfully";
    }

    public boolean isBlocked(String peerId){
        return blockedPeers.contains(peerId);
    }





    public synchronized void clearAll(){
        blockedPeers.clear();
        delayMs = 0;
        dropProbability = 0.0;
        clockSkewMs = 0;
    }
}
