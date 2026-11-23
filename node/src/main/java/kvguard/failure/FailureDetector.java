package kvguard.failure;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import kvguard.NodeLog;
import kvguard.proto.FailureDetectionServiceGrpc;
import kvguard.proto.PingRequest;
import kvguard.proto.PingResponse;


public class FailureDetector implements AutoCloseable {
    private final String nodeId;
    private final FaultManager faults;
    private static final long HEARTBEAT_INTERVAL_MS = 1000;
    private static final long RPC_TIMEOUT_MS = 500;
    private static final int FAILURE_THRESHOLD = 3;
    
    private final Map<String, ManagedChannel> channels = 
              new ConcurrentHashMap<>();
    public void setFailureListener(Consumer<String> listener){
        this.failureListener = listener != null ? listener : peerId -> {};
    }
    public void setRecoveryListener(Consumer<String> listener){
        this.recoveryListener = listener != null ? listener : peerId -> {};
    }
    private final Map<String, peerHealth> peerHealthMap = 
              new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = 
              Executors.newScheduledThreadPool(2);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private volatile Consumer<String> failureListener = peerId -> {};
    private volatile Consumer<String> recoveryListener = peerId -> {};
    public FailureDetector(String nodeId, Map<String, String> peers, FaultManager faults){
        this.nodeId = nodeId;
        this.faults = faults;

        for(Map.Entry<String, String> peer : peers.entrySet()){
            String peerId = peer.getKey();
            String address = peer.getValue();

            if(peerId.equals(nodeId)){
                continue;
            }

            ManagedChannel channel = 
                    ManagedChannelBuilder.forTarget(address)
                    .usePlaintext()
                    .build();

            channels.put(peerId, channel);
            peerHealthMap.put(peerId, new peerHealth(peerId));
        }

        NodeLog.info(nodeId, "FAILURE DETECTOR Started. Monitoring "
                     + peerHealthMap.size() + " peers.");
    }

    public long getCurrentTimeMillis(){
        return faults.currentTimeMillis();
    }

    public void start(){
        scheduler.scheduleWithFixedDelay
                   (this::checkPeers,
                    0,
                    HEARTBEAT_INTERVAL_MS,
                    TimeUnit.MILLISECONDS);
    }

    private void checkPeers(){
        if(closed.get()){
            return;
        }

    for (Map.Entry<String, ManagedChannel> peer : channels.entrySet()) {

            scheduler.execute(() ->
                    sendHeartbeat(peer.getKey(), peer.getValue())
            );
        }    
    }

private void sendHeartbeat(String peerId, ManagedChannel channel) {

        if (closed.get()) {
            return;
        }

        try {
            try {
            if (!faults.beforeSend(peerId)) {
                peerHealth health = peerHealthMap.get(peerId);
                if(health != null){
                    boolean suspected = health.recordFailure(FAILURE_THRESHOLD);
                    if(suspected){
                        NodeLog.info(nodeId, "Failure peer= "+ peerId + " suspected");
                try {
                  try {
                    failureListener.accept(peerId);
                  } catch (RuntimeException listenerError) {
                    NodeLog.info(nodeId, "Failure listener failed for peer=" + peerId);
                  }
                } catch (RuntimeException listenerError) {
                   NodeLog.info(nodeId, "Failure listener failed for peer=" + peerId);
                }                    }
                }
               return;
            }
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
               return;
           }
            PingRequest request = PingRequest.newBuilder()
                    .setSenderId(nodeId)
                    .setSentMs(System.currentTimeMillis())
                    .build();

            PingResponse response =
                    FailureDetectionServiceGrpc.newBlockingStub(channel)
                            .withDeadlineAfter(
                                    RPC_TIMEOUT_MS,
                                    TimeUnit.MILLISECONDS
                            )
                            .ping(request);

            peerHealth health = peerHealthMap.get(peerId);

            if (health != null) {

                boolean recovered = health.recordSuccess();

                if (recovered) {
                    NodeLog.info(nodeId,
                            "FAILURE peer=" + peerId + " RECOVERED");
                    try{
                        recoveryListener.accept(peerId);
                        
                    }catch(RuntimeException e){
                        NodeLog.info(nodeId, "Recovery listener failed for peer=" + peerId);
                    }
                }
            }

        } catch (RuntimeException e) {

            peerHealth health = peerHealthMap.get(peerId);

            if (health != null) {

                boolean suspected =
                        health.recordFailure(FAILURE_THRESHOLD);

                if (suspected) {
                    NodeLog.info(nodeId,
                            "FAILURE peer=" + peerId + " SUSPECTED");
                try{
                       try {
                         failureListener.accept(peerId);
                       } catch (RuntimeException listenerError) {
                          NodeLog.info(nodeId, "Failure listener failed for peer=" + peerId);
                       }
                } catch(RuntimeException listenerError){
                        NodeLog.info(nodeId, "Failure listener failed for peer=" + peerId);
                    }
                }
            }
        }
    }

    public Map<String, peerHealth> getPeerHealthMap() {
        return Map.copyOf(peerHealthMap);
    }

    public String getPeerStatus(String peerId){
        peerHealth health = peerHealthMap.get(peerId);
        if(health == null){
            return "UNKNOWN";
        }
        return health.getState().name();
    }
    
    public void printPeerStatuses() {
    for (String peerId : peerHealthMap.keySet()) {
        NodeLog.info(nodeId,
                "Peer " + peerId + " status: " + getPeerStatus(peerId));
    }
    }

    public void close() {

        if (!closed.compareAndSet(false, true)) {
            return;
        }

        scheduler.shutdownNow();

        for (ManagedChannel channel : channels.values()) {
            channel.shutdownNow();
        }

        NodeLog.info(nodeId, "FAILURE DETECTOR stopped");
    }
}
