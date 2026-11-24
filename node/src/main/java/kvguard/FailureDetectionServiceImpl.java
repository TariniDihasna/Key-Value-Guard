package kvguard;

import io.grpc.stub.StreamObserver;
import kvguard.clock.ClockService;
import kvguard.consensus.ConsensusModule;
import kvguard.failure.FaultManager;
import kvguard.proto.FailureDetectionServiceGrpc;
import kvguard.proto.PingRequest;
import kvguard.proto.PingResponse;

/** Answers "are you alive?" pings from other nodes and from the client. */
public class FailureDetectionServiceImpl extends FailureDetectionServiceGrpc.FailureDetectionServiceImplBase {

    private final String nodeId;
    private final ClockService clockService;
    private final ConsensusModule consensus;
    private final FaultManager faults;

    public FailureDetectionServiceImpl(String nodeId, ClockService clockService,
                                       ConsensusModule consensus, FaultManager faults) {
        this.nodeId = nodeId;
        this.clockService = clockService;
        this.consensus = consensus;
        this.faults = faults;
    }

    @Override
    public void ping(PingRequest request, StreamObserver<PingResponse> responseObserver) {
        // Partition: ignore traffic from a blocked node.
        if (faults.isBlocked(request.getSenderId())) {
            responseObserver.onError(io.grpc.Status.UNAVAILABLE
                    .withDescription("blocked by fault injection").asRuntimeException());
            return;
        }

        clockService.lamportOnReceive(request.getLamport());
        long responseLamport = clockService.lamportTick();

        responseObserver.onNext(PingResponse.newBuilder()
                .setNodeId(nodeId)
                .setRole(consensus.roleName())
                .setTerm(consensus.term())
                .setLamport(responseLamport)
                .build());
        responseObserver.onCompleted();
    }
}