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

}
