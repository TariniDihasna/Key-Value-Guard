package kvguard.failure;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import kvguard.NodeLog;
import kvguard.proto.FailureDetectionServiceGrpc;
import kvguard.proto.PingRequest;
import kvguard.proto.PingResponse;


public class FailureDetector {
    private final String nodeId;

    private static final long HEARTBEAT_INTERVAL_Ms = 1000;
    private static final long RPC_TIMEOUT_Ms = 500;
    private static final int FAILURE_THRESHOLD = 3;
    
    private final Map<String, ManagedChannel> channels = 
              new ConcurrentHashMap<>();
    private final Map<String, peerHealth> peerHealthMap = 
              new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = 
              Executors.newScheduledThreadPool(2);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    public FailureDetector(String nodeId, Map<String, String> peers){
        this.nodeId = nodeId;

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

    public void start(){
        scheduler.scheduleWithFixedDelay
                   (this::checkPeers,
                    0,
                    HEARTBEAT_INTERVAL_Ms,
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
                }
            }
        }
    }

    public Map<String, peerHealth> getPeerHealthMap() {
        return Map.copyOf(peerHealthMap);
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
