package kvguard;

import io.grpc.stub.StreamObserver;
import kvguard.proto.*;
import kvguard.store.KeyValueStore;
import kvguard.store.KeyValueStore.ApplyResult;
import kvguard.store.KeyValueStore.Entry;
import kvguard.store.Replicator;
import kvguard.store.LeaderInfo;
import kvguard.proto.ReadMode;
import kvguard.store.ReplicationException;
import kvguard.clock.ClockService;

import java.util.Optional;

public class ClientServiceImpl extends ClientServiceGrpc.ClientServiceImplBase {

    private final String nodeId;
    private final KeyValueStore store;
    private final Replicator replicator;
    private final LeaderInfo leader;
    private final ClockService clockService;

    public ClientServiceImpl(String nodeId, KeyValueStore store,Replicator replicator, LeaderInfo leader,  ClockService clockService ) {
        this.nodeId = nodeId;
        this.store = store;
        this.replicator = replicator;
        this.leader = leader;
        this.clockService = clockService;
    }

    @Override
    public void submit(SubmitRequest request, StreamObserver<SubmitResponse> responseObserver) {
        Operation op = request.getOp();
        if(op != Operation.OP_PUT && op != Operation.OP_DELETE && op != Operation.OP_CAS) {
            responseObserver.onNext(SubmitResponse.newBuilder().setStatus(Status.STATUS_UNKNOWN).setMessage("Unsupported operation: " + op).setServedBy(nodeId).build());
            responseObserver.onCompleted();
            return;
        }

        if(!leader.isLeader()){
            responseObserver.onNext(SubmitResponse.newBuilder().setStatus(Status.NOT_LEADER).setLeaderHint(leader.leaderId() == null ? "" : leader.leaderId()).setServedBy(nodeId).build());
            responseObserver.onCompleted();
            return;
        }

        LogEntry entry = LogEntry.newBuilder().setRequestId(request.getRequestId()).setClientId(request.getClientId()).setOp(op).setKey(request.getKey()).setValue(request.getValue()).setExpectedVersion(request.getExpectedVersion()).build();
        ApplyResult result;
        try {
            result = replicator.replicate(entry);
        }catch (ReplicationException e){
            Status failed = e.reason() == ReplicationException.Reason.NOT_LEADER ? Status.NOT_LEADER : Status.NO_QUORUM;
            NodeLog.info(nodeId,"submit request = " + request.getRequestId() + "->" + failed);
            responseObserver.onNext(SubmitResponse.newBuilder().setStatus(failed).setLeaderHint(e.leaderHint()).setMessage(e.getMessage()).setServedBy(nodeId).build());
            responseObserver.onCompleted();
            return;
        }

        Status status = toStatus(result);
        NodeLog.info(nodeId, "submit op = " + request.getOp() + " key = " + request.getKey()
                + " request = " + request.getRequestId() + " -> " + status);

        responseObserver.onNext(SubmitResponse.newBuilder()
                .setStatus(status)
                .setLogIndex(result.version())
                .setCurrentVersion(result.version())
                .setServedBy(nodeId)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        if(request.getMode() == ReadMode.READ_STRONG && !leader.isLeader()) {
            responseObserver.onNext(GetResponse.newBuilder().setStatus(Status.NOT_LEADER).setLeaderHint(leader.leaderId() == null ? "" : leader.leaderId()).setServedBy(nodeId).build());
            responseObserver.onCompleted();
            return;
        }

        if(request.getMode() == ReadMode.READ_STRONG && !leader.confirmLeadership()){
            responseObserver.onNext(GetResponse.newBuilder().setStatus(Status.NO_QUORUM).setServedBy(nodeId).build());
            responseObserver.onCompleted();
            return;
        }
        Optional<Entry> entry = store.get(request.getKey());
        GetResponse.Builder reply = GetResponse.newBuilder()
                .setAppliedIndex(store.lastApplied())
                .setServedBy(nodeId);
        if (entry.isPresent()) {
            reply.setStatus(Status.OK)
                    .setFound(true)
                    .setValue(entry.get().value())
                    .setVersion(entry.get().version());
        } else {
            reply.setStatus(Status.KEY_NOT_FOUND).setFound(false);
        }
        responseObserver.onNext(reply.build());
        responseObserver.onCompleted();
    }

    private static Status toStatus(ApplyResult result) {
        if (result.duplicate()) {
            return Status.DUPLICATE;
        }
        return switch (result.result()) {
            case OK -> Status.OK;
            case VERSION_MISMATCH -> Status.VERSION_MISMATCH;
            case KEY_NOT_FOUND -> Status.KEY_NOT_FOUND;
        };
    }
}
