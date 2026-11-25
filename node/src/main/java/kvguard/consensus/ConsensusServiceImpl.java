package kvguard.consensus;

import io.grpc.stub.StreamObserver;
import kvguard.failure.FaultManager;
import kvguard.proto.AppendEntriesRequest;
import kvguard.proto.AppendEntriesResponse;
import kvguard.proto.ConsensusServiceGrpc;
import kvguard.proto.RequestVoteRequest;
import kvguard.proto.RequestVoteResponse;

/** gRPC adapter for Role 2 Raft RPCs. */
public final class ConsensusServiceImpl extends ConsensusServiceGrpc.ConsensusServiceImplBase {
    private final ConsensusModule consensus;
    private final FaultManager faults;

    public ConsensusServiceImpl(ConsensusModule consensus, FaultManager faults) {
        this.consensus = consensus;
        this.faults = faults;
    }

    @Override
    public void requestVote(RequestVoteRequest request, StreamObserver<RequestVoteResponse> observer) {
        if (faults.isBlocked(request.getCandidateId())) {
            observer.onError(io.grpc.Status.UNAVAILABLE
                    .withDescription("blocked by fault injection").asRuntimeException());
            return;
        }
        observer.onNext(consensus.onRequestVote(request));
        observer.onCompleted();
    }

    @Override
    public void appendEntries(AppendEntriesRequest request, StreamObserver<AppendEntriesResponse> observer) {
        if (faults.isBlocked(request.getLeaderId())) {
            observer.onError(io.grpc.Status.UNAVAILABLE
                    .withDescription("blocked by fault injection").asRuntimeException());
            return;
        }
        observer.onNext(consensus.onAppendEntries(request));
        observer.onCompleted();
    }
}