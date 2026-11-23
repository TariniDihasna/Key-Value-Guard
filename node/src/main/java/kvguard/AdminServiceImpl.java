package kvguard;

import io.grpc.stub.StreamObserver;
import kvguard.failure.FaultManager;
import kvguard.proto.AdminServiceGrpc;
import kvguard.proto.FaultAck;
import kvguard.proto.FaultConfig;
import kvguard.proto.NodeStatus;
import kvguard.proto.StatusRequest;
import kvguard.store.KeyValueStore;
import kvguard.clock.ClockService;

/**
 * ROLE 3 starting point: status and fault injection.
 *
 * GetStatus works in a basic form. SetFault is a placeholder: Role 3 should implement
 * partition (blocked_peers), delay_ms, drop_probability and clear_all, and Role 1 should
 * apply clock_skew_ms to the node's clock.
 */
public class AdminServiceImpl extends AdminServiceGrpc.AdminServiceImplBase {

    private final String nodeId;
    private final KeyValueStore store;
    private final FaultManager faults;
    private final ClockService clockService;

    public AdminServiceImpl(String nodeId, KeyValueStore store, FaultManager faults, ClockService clockService) {
        this.nodeId = nodeId;
        this.store = store;
        this.faults = faults;
        this.clockService = clockService;
    }

    @Override
    public void getStatus(StatusRequest request, StreamObserver<NodeStatus> responseObserver) {
        responseObserver.onNext(NodeStatus.newBuilder()
                .setNodeId(nodeId)
                .setRole("FOLLOWER")
                .setLastApplied(store.lastApplied())
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void setFault(FaultConfig request, StreamObserver<FaultAck> responseObserver) {
        try {
            String message = faults.configure(request);
            clockService.setSkewMillis(faults.getClockSkewMs());
            responseObserver.onNext(FaultAck.newBuilder()
                    .setOk(true)
                    .setMessage(message)
                    .build());
            
        } catch (IllegalArgumentException e) {
            responseObserver.onNext(FaultAck.newBuilder()
                .setOk(false)
                .setMessage(e.getMessage())
                .build());
        }
        responseObserver.onCompleted();
    }
}
