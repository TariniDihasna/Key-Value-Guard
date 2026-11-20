package kvguard;

import io.grpc.stub.StreamObserver;
import kvguard.proto.*;
import kvguard.store.KeyValueStore;
import kvguard.store.KeyValueStore.ApplyResult;
import kvguard.store.KeyValueStore.Entry;
import kvguard.store.Replicator;

import java.util.Optional;

public class ClientServiceImpl extends ClientServiceGrpc.ClientServiceImplBase {

    private final String nodeId;
    private final KeyValueStore store;
    private final Replicator replicator;

    public ClientServiceImpl(String nodeId, KeyValueStore store,Replicator replicator) {
        this.nodeId = nodeId;
        this.store = store;
        this.replicator = replicator;
    }

    @Override
    public void submit(SubmitRequest request, StreamObserver<SubmitResponse> responseObserver) {
        Operation op = request.getOp();
        if(op != Operation.OP_PUT && op != Operation.OP_DELETE && op != Operation.OP_CAS) {
            responseObserver.onNext(SubmitResponse.newBuilder().setStatus(Status.STATUS_UNKNOWN).setMessage("Unsupported operation: " + op).setServedBy(nodeId).build());
            responseObserver.onCompleted();
            return;
        }

        LogEntry entry = LogEntry.newBuilder().setRequestId(request.getRequestId()).setClientId(request.getClientId()).setOp(op).setKey(request.getKey()).setValue(request.getValue()).setExpectedVersion(request.getExpectedVersion()).build();
        ApplyResult result = replicator.replicate(entry);

        Status status = toStatus(result);
        NodeLog.info(nodeId, "submit op=" + request.getOp() + " key=" + request.getKey()
                + " request=" + request.getRequestId() + " -> " + status);

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
