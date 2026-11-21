
package kvguard.clock;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import kvguard.proto.TimeRequest;
import kvguard.proto.TimeResponse;
import kvguard.proto.TimeServiceGrpc;

import java.util.concurrent.TimeUnit;

/**
 * Implements the clock operations for a cluster node.
 */
public class ClockServiceImpl implements ClockService {

    private final String nodeId;
    private final String[] peerAddresses;
    private final LamportClock lamportClock;

    private volatile long skewMillis = 0;
    private volatile long synchronizationOffsetMillis = 0;

    public ClockServiceImpl(String nodeId, String peers) {
        this.nodeId = nodeId;
        this.peerAddresses = peers.split(",");
        this.lamportClock = new LamportClock();
    }

    @Override
    public long lamportTick() {
        return lamportClock.tick();
    }

    @Override
    public long lamportOnReceive(long remote) {
        return lamportClock.onReceive(remote);
    }

    @Override
    public long now() {
        return System.currentTimeMillis()
                + skewMillis
                + synchronizationOffsetMillis;
    }

    @Override
    public void setSkewMillis(long skew) {
        this.skewMillis = skew;
    }

    @Override
    public void syncWith(String timeServerId) {

        String address = findTimeServerAddress(timeServerId);

        String[] hostAndPort = address.split(":");

        if (hostAndPort.length != 2) {
            throw new IllegalArgumentException(
                    "Invalid peer address: " + address);
        }

        String host = hostAndPort[0];
        int port = Integer.parseInt(hostAndPort[1]);

        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(host, port)
                .usePlaintext()
                .build();

        try {
            // Record the local time before sending the request.
            long t0 = now();

            TimeServiceGrpc.TimeServiceBlockingStub stub =
                    TimeServiceGrpc.newBlockingStub(channel)
                            .withDeadlineAfter(2, TimeUnit.SECONDS);

            TimeRequest request = TimeRequest.newBuilder()
                    .setSenderId(nodeId)
                    .build();

            // Ask the other node for its current time.
            TimeResponse response = stub.getTime(request);

            // Record the local time after receiving the response.
            long t1 = now();

            // Update the Lamport clock using the remote timestamp.
            lamportOnReceive(response.getLamport());

            // Cristian-style clock synchronization.
            long estimatedServerTime =
                    response.getServerTimeMs() + (t1 - t0) / 2;

            synchronizationOffsetMillis =
                    estimatedServerTime - t1;

        } catch (StatusRuntimeException e) {
            throw new IllegalStateException(
                    "Could not synchronize with " + timeServerId, e);
        } finally {
            channel.shutdownNow();
        }
    }

    /**
     * Finds a node's address using the order in the --peers argument.
     *
     * For example:
     * node1 = first address
     * node2 = second address
     */
    private String findTimeServerAddress(String timeServerId) {

        if (timeServerId == null || !timeServerId.matches("node[1-9][0-9]*")) {
            throw new IllegalArgumentException(
                    "Invalid time server ID: " + timeServerId);
        }

        int nodeNumber = Integer.parseInt(
                timeServerId.substring(4));

        int index = nodeNumber - 1;

        if (index < 0 || index >= peerAddresses.length) {
            throw new IllegalArgumentException(
                    "No address configured for " + timeServerId);
        }

        return peerAddresses[index].trim();
    }
}

