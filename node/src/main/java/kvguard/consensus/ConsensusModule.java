package kvguard.consensus;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import kvguard.NodeLog;
import kvguard.failure.FaultManager;
import kvguard.proto.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Educational Raft-style consensus module for KeyValueGuard.
 * Implements persistent term/vote/log, randomized elections, RequestVote,
 * AppendEntries heartbeats and log replication, majority commit, and catch-up.
 * This is coursework code, not a production consensus library.*/

public final class ConsensusModule implements AutoCloseable {
    public enum Role { FOLLOWER, CANDIDATE, LEADER }
    public record Peer(String id, String target) { }
    public static final class NotLeaderException extends Exception {
        private final String leaderId;
        public NotLeaderException(String leaderId) { super("Not leader; leader=" + leaderId); this.leaderId=leaderId; }
        public String leaderId() { return leaderId; }
    }
    public static final class NoQuorumException extends Exception {
        public NoQuorumException(String message) { super(message); }
    }

    private final String nodeId;
    private final List<Peer> peers;
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final Consumer<LogEntry> applyCommitted;
    private final FaultManager faults;
    private final Path stateFile;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final ExecutorService rpcPool = Executors.newCachedThreadPool();
    private final Object lock = new Object();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final List<LogEntry> log = new ArrayList<>(); // index 1 maps to element 0
    private final Map<String, Long> nextIndex = new HashMap<>();
    private final Map<String, Long> matchIndex = new HashMap<>();

    private long currentTerm = 0;
    private String votedFor = "";
    private Role role = Role.FOLLOWER;
    private String leaderId = "";
    private long commitIndex = 0;
    private long lastApplied = 0;
    private long lastContactNanos = System.nanoTime();
    private long electionDeadlineNanos;
    private int votesReceived = 0;
    private final int majority;

    public ConsensusModule(String nodeId, List<Peer> peers, String dataDir, Consumer<LogEntry> applier, FaultManager faults) throws IOException {
        this.nodeId=Objects.requireNonNull(nodeId);
        this.peers=List.copyOf(peers);
        this.applyCommitted=Objects.requireNonNull(applier);
        this.faults=Objects.requireNonNull(faults);
        this.majority=(peers.size()/2)+1;
        if (peers.stream().noneMatch(p -> p.id().equals(nodeId))) throw new IllegalArgumentException("Peers list must include this node: "+nodeId);
        Path dir=Paths.get(dataDir); Files.createDirectories(dir); this.stateFile=dir.resolve("raft-state.bin");
        loadState(); resetElectionDeadline();
        for (Peer p: peers) if (!p.id().equals(nodeId)) channels.put(p.id(), ManagedChannelBuilder.forTarget(p.target()).usePlaintext().build());
        scheduler.scheduleWithFixedDelay(this::tick, 50, 50, TimeUnit.MILLISECONDS);
        NodeLog.info(nodeId,"CONSENSUS started peers="+peers.size()+" majority="+majority+" term="+currentTerm+" log="+log.size());
    }

    public ConsensusServiceImpl service() { return new ConsensusServiceImpl(this); }
    public String roleName() { synchronized(lock){ return role.name(); } }
    public long term() { synchronized(lock){ return currentTerm; } }
    public String leaderId() { synchronized(lock){ return leaderId; } }
    public long commitIndex() { synchronized(lock){ return commitIndex; } }
    public long lastApplied() { synchronized(lock){ return lastApplied; } }
    public long lastLogIndex() { synchronized(lock){ return log.size(); } }
    public boolean isLeader() { synchronized(lock){ return role==Role.LEADER; } }

    private void tick() {
        if (closed.get()) return;
        boolean elect=false, heartbeat=false;
        synchronized(lock) {
            long now=System.nanoTime();
            if (role==Role.LEADER) heartbeat=true;
            else if (now>=electionDeadlineNanos) elect=true;
        }
        if (elect) startElection();
        if (heartbeat) replicateAll(false);
    }

    private void resetElectionDeadline() {
        long millis=900 + ThreadLocalRandom.current().nextLong(650);
        electionDeadlineNanos=System.nanoTime()+TimeUnit.MILLISECONDS.toNanos(millis);
    }

    private void startElection() {
        final long electionTerm;
        final long lastIdx,lastTerm;
        synchronized(lock) {
            if (closed.get() || role==Role.LEADER) return;
            role=Role.CANDIDATE; currentTerm++; votedFor=nodeId; leaderId=""; votesReceived=1;
            electionTerm=currentTerm; lastIdx=log.size(); lastTerm=termAt(lastIdx); resetElectionDeadline(); persistQuietly();
            NodeLog.info(nodeId,"CONSENSUS election started term="+currentTerm+" lastLog="+lastIdx+"/"+lastTerm);
            if (votesReceived>=majority) becomeLeaderLocked();
        }
        List<Future<Boolean>> futures=new ArrayList<>();
        for (Peer p: peers) if (!p.id().equals(nodeId)) futures.add(rpcPool.submit(() -> requestVote(p,electionTerm,lastIdx,lastTerm)));
        scheduler.execute(() -> {
            for (Future<Boolean> f:futures) {
                try {
                    if (f.get(700,TimeUnit.MILLISECONDS)) {
                        synchronized(lock) {
                            if (role!=Role.CANDIDATE || currentTerm!=electionTerm) return;
                            votesReceived++;
                            if (votesReceived>=majority) { becomeLeaderLocked(); break; }
                        }
                    }
                } catch(Exception ignored) { }
            }
        });
    }

    private boolean requestVote(Peer p,long term,long lastIdx,long lastTerm) {
        try {
            RequestVoteResponse r=ConsensusServiceGrpc.newBlockingStub(channels.get(p.id())).withDeadlineAfter(450,TimeUnit.MILLISECONDS)
                    .requestVote(RequestVoteRequest.newBuilder().setTerm(term).setCandidateId(nodeId).setLastLogIndex(lastIdx).setLastLogTerm(lastTerm).build());
            synchronized(lock) {
                if (r.getTerm()>currentTerm) stepDownLocked(r.getTerm(),"");
                return r.getVoteGranted() && currentTerm==term;
            }
        } catch(Exception e) { return false; }
    }

    public RequestVoteResponse onRequestVote(RequestVoteRequest req) {
        synchronized(lock) {
            if (req.getTerm()>currentTerm) stepDownLocked(req.getTerm(),"");
            boolean grant=false;
            if (req.getTerm()==currentTerm) {
                long myLast=log.size(), myTerm=termAt(myLast);
                boolean upToDate=req.getLastLogTerm()>myTerm || (req.getLastLogTerm()==myTerm && req.getLastLogIndex()>=myLast);
                if (upToDate && (votedFor.isEmpty() || votedFor.equals(req.getCandidateId()))) {
                    votedFor=req.getCandidateId(); role=Role.FOLLOWER; leaderId=""; resetElectionDeadline(); persistQuietly(); grant=true;
                }
            }
            return RequestVoteResponse.newBuilder().setTerm(currentTerm).setVoteGranted(grant).build();
        }
    }

    public AppendEntriesResponse onAppendEntries(AppendEntriesRequest req) {
        synchronized(lock) {
            if (req.getTerm()<currentTerm) return appendReply(false,log.size());
            if (req.getTerm()>currentTerm || role!=Role.FOLLOWER) stepDownLocked(req.getTerm(),req.getLeaderId());
            leaderId=req.getLeaderId(); lastContactNanos=System.nanoTime(); resetElectionDeadline();
            long prev=req.getPrevLogIndex();
            if (prev>log.size() || (prev>0 && termAt(prev)!=req.getPrevLogTerm())) return appendReply(false,Math.min(log.size(),prev-1));
            int offset=0;
            for (LogEntry incoming:req.getEntriesList()) {
                long index=incoming.getIndex();
                if (index<=log.size()) {
                    if (termAt(index)!=incoming.getTerm()) {
                        if (index<=commitIndex) return appendReply(false,log.size());
                        while(log.size()>=index) log.remove(log.size()-1);
                        log.add(incoming); offset++;
                    }
                } else { log.add(incoming); offset++; }
            }
            if (offset>0) persistQuietly();
            if (req.getLeaderCommit()>commitIndex) {
                commitIndex=Math.min(req.getLeaderCommit(),log.size()); applyCommittedLocked(); persistQuietly();
            }
            return appendReply(true,prev+req.getEntriesCount());
        }
    }

    private AppendEntriesResponse appendReply(boolean success,long match) {
        return AppendEntriesResponse.newBuilder().setTerm(currentTerm).setSuccess(success).setMatchIndex(match).build();
    }

    private void becomeLeaderLocked() {
        role=Role.LEADER; leaderId=nodeId;
        long next=log.size()+1L;
        for(Peer p:peers) if(!p.id().equals(nodeId)){nextIndex.put(p.id(),next);matchIndex.put(p.id(),0L);}
        NodeLog.info(nodeId,"CONSENSUS became LEADER term="+currentTerm+" majority="+majority);
        // A current-term no-op establishes a commit point for entries from previous terms.
        LogEntry noop=LogEntry.newBuilder().setTerm(currentTerm).setIndex(log.size()+1L).setOp(Operation.OP_NOOP).setRequestId("raft-noop-"+nodeId+"-"+currentTerm).build();
        log.add(noop); persistQuietly();
        scheduler.execute(this::replicateLeaderNoop);
    }

    private void replicateLeaderNoop() {
        int replicated=1;
        for(Peer p:peers) if(!p.id().equals(nodeId) && replicatePeer(p,true)) replicated++;
        synchronized(lock) {
            if(role==Role.LEADER && replicated>=majority && commitIndex<log.size()) {
                commitIndex=log.size(); applyCommittedLocked(); persistQuietly();
                NodeLog.info(nodeId,"CONSENSUS leader no-op committed index="+commitIndex+" term="+currentTerm);
            }
        }
        replicateAll(false);
    }
  
    public long propose(LogEntry proposed) throws NotLeaderException, NoQuorumException {
        final LogEntry entry;
        synchronized(lock) {
            if(role!=Role.LEADER) throw new NotLeaderException(leaderId);
            entry=proposed.toBuilder().setTerm(currentTerm).setIndex(log.size()+1L).build();
            log.add(entry); persistQuietly();
        }
        int replicated=1;
        for(Peer p:peers) if(!p.id().equals(nodeId)) if(replicatePeer(p,true)) replicated++;
        synchronized(lock) {
            if(role!=Role.LEADER || entry.getTerm()!=currentTerm) throw new NoQuorumException("Leadership changed while proposing");
            if(replicated<majority) throw new NoQuorumException("Majority unavailable: replicated="+replicated+" required="+majority);
            if(entry.getIndex()>commitIndex) { commitIndex=entry.getIndex(); applyCommittedLocked(); persistQuietly(); }
        }
        replicateAll(false); // publish updated leaderCommit to followers
        NodeLog.info(nodeId,"CONSENSUS committed index="+entry.getIndex()+" term="+entry.getTerm()+" op="+entry.getOp());
        return entry.getIndex();
    }

    /** Leader must contact a majority before serving a strong read. */
    public boolean confirmLeadership() {
        synchronized(lock){ if(role!=Role.LEADER) return false; }
        int count=1;
        for(Peer p:peers) if(!p.id().equals(nodeId) && replicatePeer(p,false)) count++;
        synchronized(lock){ return role==Role.LEADER && count>=majority; }
    }

    private void replicateAll(boolean commitAfter) {
        List<Peer> copy=peers.stream().filter(p->!p.id().equals(nodeId)).toList();
        for(Peer p:copy) replicatePeer(p,commitAfter);
    }

    private boolean replicatePeer(Peer p,boolean allowCommit) {
        for(int attempt=0;attempt<12;attempt++) {
            AppendEntriesRequest req;
            synchronized(lock) {
                if(role!=Role.LEADER) return false;
                long next=nextIndex.getOrDefault(p.id(),(long)log.size()+1);
                next=Math.max(1,Math.min(next,log.size()+1L));
                long prev=next-1;
                AppendEntriesRequest.Builder b=AppendEntriesRequest.newBuilder().setTerm(currentTerm).setLeaderId(nodeId)
                        .setPrevLogIndex(prev).setPrevLogTerm(termAt(prev)).setLeaderCommit(commitIndex);
                for(long i=next;i<=log.size();i++) b.addEntries(log.get((int)i-1));
                req=b.build();
            }
            try {
                AppendEntriesResponse r=ConsensusServiceGrpc.newBlockingStub(channels.get(p.id())).withDeadlineAfter(500,TimeUnit.MILLISECONDS).appendEntries(req);
                synchronized(lock) {
                    if(r.getTerm()>currentTerm){stepDownLocked(r.getTerm(),"");return false;}
                    if(role!=Role.LEADER) return false;
                    if(r.getSuccess()) {
                        long matched=req.getPrevLogIndex()+req.getEntriesCount();
                        matchIndex.put(p.id(),Math.max(matchIndex.getOrDefault(p.id(),0L),matched));
                        nextIndex.put(p.id(),matched+1);
                        return true;
                    }
                    long old=nextIndex.getOrDefault(p.id(),(long)log.size()+1);
                    nextIndex.put(p.id(),Math.max(1,Math.min(old-1,r.getMatchIndex()+1)));
                }
            } catch(StatusRuntimeException e) { return false; }
        }
        return false;
    }

}
