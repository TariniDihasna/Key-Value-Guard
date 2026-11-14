package kvguard;

import io.grpc.stub.StreamObserver;
import kvguard.proto.FailureDetectionServiceGrpc;
import kvguard.proto.PingRequest;
import kvguard.proto.PingResponse;

/**
 * ROLE 3 starting point: answers "are you alive?" pings from other nodes.
 *
 * Next steps for Role 3: build the sender side (ping every peer every 100 ms, mark a peer
 * suspected after a timeout, call listeners) in the failure package. Role 2 will fill in the
 * real role and term here once consensus exists.
 */
public class FailureDetectionServiceImpl extends FailureDetectionServiceGrpc.FailureDetectionServiceImplBase {

    private final String nodeId;

    public FailureDetectionServiceImpl(String nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public void ping(PingRequest request, StreamObserver<PingResponse> responseObserver) {
        responseObserver.onNext(PingResponse.newBuilder()
                .setNodeId(nodeId)
                .setRole("FOLLOWER")   // TODO (Role 2): real role from the consensus module
                .setTerm(0)            // TODO (Role 2): real term
                .setLamport(request.getLamport()) // TODO (Role 1): real Lamport clock
                .build());
        responseObserver.onCompleted();
    }
}
