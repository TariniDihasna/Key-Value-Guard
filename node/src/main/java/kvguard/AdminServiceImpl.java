package kvguard;

import io.grpc.stub.StreamObserver;
import kvguard.clock.ClockService;
import kvguard.consensus.ConsensusModule;
import kvguard.failure.FailureDetector;
import kvguard.failure.FaultManager;
import kvguard.proto.AdminServiceGrpc;
import kvguard.proto.FaultAck;
import kvguard.proto.FaultConfig;
import kvguard.proto.NodeStatus;
import kvguard.proto.StatusRequest;
import kvguard.store.KeyValueStore;

/** Status and fault injection for scripts and the demo. */
public class AdminServiceImpl extends AdminServiceGrpc.AdminServiceImplBase {

    private final String nodeId;
    private final KeyValueStore store;
    private final FaultManager faults;
    private final ClockService clockService;
    private final ConsensusModule consensus;
    private final FailureDetector failureDetector;

    public AdminServiceImpl(String nodeId, KeyValueStore store, FaultManager faults,
                            ClockService clockService, ConsensusModule consensus,
                            FailureDetector failureDetector) {
        this.nodeId = nodeId;
        this.store = store;
        this.faults = faults;
        this.clockService = clockService;
        this.consensus = consensus;
        this.failureDetector = failureDetector;
    }

    @Override
    public void getStatus(StatusRequest request, StreamObserver<NodeStatus> responseObserver) {
        responseObserver.onNext(NodeStatus.newBuilder()
                .setNodeId(nodeId)
                .setRole(consensus.roleName())
                .setTerm(consensus.term())
                .setLeaderId(consensus.leaderId())
                .setCommitIndex(consensus.commitIndex())
                .setLastApplied(store.lastApplied())
                .setLastLogIndex(consensus.lastLogIndex())
                .setLamport(clockService.lamportTick())
                .setClockSkewMs(faults.getClockSkewMs())
                .addAllSuspectedPeers(failureDetector.suspectedPeers())
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void setFault(FaultConfig request, StreamObserver<FaultAck> responseObserver) {
        try {
            String message = faults.configure(request);
            clockService.setSkewMillis(faults.getClockSkewMs());
            responseObserver.onNext(FaultAck.newBuilder().setOk(true).setMessage(message).build());
        } catch (IllegalArgumentException e) {
            responseObserver.onNext(FaultAck.newBuilder().setOk(false).setMessage(e.getMessage()).build());
        }
        responseObserver.onCompleted();
    }
}