package kvguard;

import io.grpc.stub.StreamObserver;
import kvguard.proto.FailureDetectionServiceGrpc;
import kvguard.proto.PingRequest;
import kvguard.proto.PingResponse;
import kvguard.clock.ClockService;

/**
 * ROLE 3 starting point: answers "are you alive?" pings from other nodes.
 *
 * Next steps for Role 3: build the sender side (ping every peer every 100 ms, mark a peer
 * suspected after a timeout, call listeners) in the failure package. Role 2 will fill in the
 * real role and term here once consensus exists.
 */
public class FailureDetectionServiceImpl extends FailureDetectionServiceGrpc.FailureDetectionServiceImplBase {

    private final String nodeId;
    private final ClockService clockService;

    public FailureDetectionServiceImpl(String nodeId, ClockService clockService ) {
        this.nodeId = nodeId;
        this.clockService = clockService;
    }

    @Override
    public void ping(
        PingRequest request,
        StreamObserver<PingResponse> responseObserver) {

    // Update the local clock when a heartbeat arrives.
    clockService.lamportOnReceive(request.getLamport());

    // Generate a new timestamp before sending the response.
    long responseLamport = clockService.lamportTick();

    PingResponse response = PingResponse.newBuilder()
            .setNodeId(nodeId)
            .setRole("FOLLOWER")
            .setTerm(0)
            .setLamport(responseLamport)
            .build();

    responseObserver.onNext(response);
    responseObserver.onCompleted();
    }
}
