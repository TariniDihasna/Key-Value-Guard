package kvguard.consensus;

import io.grpc.stub.StreamObserver;
import kvguard.proto.*;

/** gRPC adapter for Role 2 Raft RPCs. */
public final class ConsensusServiceImpl extends ConsensusServiceGrpc.ConsensusServiceImplBase {
    private final ConsensusModule consensus;
    public ConsensusServiceImpl(ConsensusModule consensus){this.consensus=consensus;}
    @Override public void requestVote(RequestVoteRequest request, StreamObserver<RequestVoteResponse> observer){
        observer.onNext(consensus.onRequestVote(request));observer.onCompleted();
    }
    @Override public void appendEntries(AppendEntriesRequest request, StreamObserver<AppendEntriesResponse> observer){
        observer.onNext(consensus.onAppendEntries(request));observer.onCompleted();
    }
}
