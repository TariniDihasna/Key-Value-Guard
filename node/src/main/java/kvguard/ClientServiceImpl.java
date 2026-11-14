package kvguard;

import io.grpc.stub.StreamObserver;
import kvguard.store.KeyValueStore;
import kvguard.store.KeyValueStore.ApplyResult;
import kvguard.store.KeyValueStore.Entry;
import kvguard.proto.ClientServiceGrpc;
import kvguard.proto.GetRequest;
import kvguard.proto.GetResponse;
import kvguard.proto.Status;
import kvguard.proto.SubmitRequest;
import kvguard.proto.SubmitResponse;

import java.util.Optional;

/**
 * ROLE 4: the API that clients call (Submit and Get).
 *
 * Right now every node applies requests directly to its own store, with no leader and no
 * replication. Next steps for Role 4:
 *   1. If this node is not the leader, answer NOT_LEADER with a leader_hint.
 *   2. Instead of calling store.put(...) directly, call consensus.propose(entry) and apply the
 *      change only when the commit callback arrives.
 *   3. Support READ_STRONG (leader only) and READ_STALE (any node).
 */
public class ClientServiceImpl extends ClientServiceGrpc.ClientServiceImplBase {

    private final String nodeId;
    private final KeyValueStore store;

    public ClientServiceImpl(String nodeId, KeyValueStore store) {
        this.nodeId = nodeId;
        this.store = store;
    }

    @Override
    public void submit(SubmitRequest request, StreamObserver<SubmitResponse> responseObserver) {
        ApplyResult result;
        switch (request.getOp()) {
            case OP_PUT -> result = store.put(request.getRequestId(), request.getKey(), request.getValue());
            case OP_DELETE -> result = store.delete(request.getRequestId(), request.getKey());
            case OP_CAS -> result = store.cas(request.getRequestId(), request.getKey(),
                    request.getValue(), request.getExpectedVersion());
            default -> {
                responseObserver.onNext(SubmitResponse.newBuilder()
                        .setStatus(Status.STATUS_UNKNOWN)
                        .setMessage("Unsupported operation: " + request.getOp())
                        .setServedBy(nodeId)
                        .build());
                responseObserver.onCompleted();
                return;
            }
        }

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
