package kvguard;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import kvguard.consensus.ConsensusModule;
import kvguard.consensus.ConsensusModule.Peer;
import kvguard.failure.FailureDetector;
import kvguard.failure.FaultManager;
import kvguard.store.ConsensusReplicator;
import kvguard.store.KeyValueStore;
import kvguard.clock.ClockService;
import kvguard.clock.ClockServiceImpl;
import kvguard.clock.TimeServiceImpl;
import kvguard.clock.LogOrderingChecker;
import kvguard.proto.LogEntry;

public class Main {

    public static void main(String[] args) throws Exception {
        Map<String, String> options = parseOptions(args);
        String nodeId = options.getOrDefault("id", "node1");
        int port = Integer.parseInt(options.getOrDefault("port", "9001"));
        String peers = options.getOrDefault("peers", "");

        KeyValueStore store = new KeyValueStore();
        ClockService clockService = new ClockServiceImpl(nodeId, peers);
        FaultManager faults = new FaultManager();
        List<Peer> clusterPeers = parsePeers(peers);

        Map<String, String> peerAddresses = new HashMap<>();
        for(Peer peer : clusterPeers){
            peerAddresses.put(peer.id(), peer.target());
        }

        FailureDetector failureDetector = new FailureDetector(nodeId, peerAddresses, faults, clockService );
        failureDetector.start();
        List<LogEntry> committedLog = Collections.synchronizedList(new ArrayList<>());
        LogOrderingChecker orderingChecker = new LogOrderingChecker();
        ConsensusReplicator replicator = new ConsensusReplicator(store);
        ConsensusModule consensus = new ConsensusModule(
                nodeId,
                clusterPeers,
                "data/" + nodeId,
                entry -> { committedLog.add(entry); replicator.onCommit(entry); },
                faults, clockService );
        replicator.attach(consensus);

        failureDetector.setFailureListener(peerId -> 
                NodeLog.info(nodeId, "Detected failure of " + peerId)
        );
        failureDetector.setRecoveryListener(peerId -> 
                NodeLog.info(nodeId, "Detected recovery of " + peerId)
        );

        Server server = ServerBuilder.forPort(port)
                .addService(new ClientServiceImpl(nodeId, store, replicator, replicator, clockService))
                .addService(consensus.service())
                .addService(new FailureDetectionServiceImpl(nodeId, clockService ))
                .addService(new AdminServiceImpl(nodeId, store, faults, clockService ))
                .addService(new TimeServiceImpl(clockService))
                .build()
                .start();

        ScheduledExecutorService background = Executors.newSingleThreadScheduledExecutor();
        if (!nodeId.equals("node1")) {
            background.scheduleWithFixedDelay(() -> {
                try {
                    clockService.syncWith("node1");
                    NodeLog.info(nodeId, "TIME synchronized with node1");
                } catch (Exception e) {
                    NodeLog.info(nodeId, "TIME sync with node1 failed: " + e.getMessage());
                }
            }, 2, 30, TimeUnit.SECONDS);
        }
        background.scheduleWithFixedDelay(
                () -> runOrderingCheck(nodeId, committedLog, orderingChecker), 30, 30, TimeUnit.SECONDS);

        NodeLog.info(nodeId, "started on port " + port + ", peers=[" + peers + "]");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            NodeLog.info(nodeId, "shutting down");
            background.shutdownNow();
            runOrderingCheck(nodeId, committedLog, orderingChecker);
            failureDetector.close();
            consensus.close();
            server.shutdown();
        }));
        server.awaitTermination();
    }

    private static List<Peer> parsePeers(String peers){
        List<Peer> list = new ArrayList<>();
        if(peers.isBlank()){
            return list;
        }
        String[] targets = peers.split(",");
        for (int i=0; i< targets.length; i++){
            list.add(new Peer("node" + (i+1),targets[i].trim()));
        }
        return list;
    }

    private static void runOrderingCheck(String nodeId, List<LogEntry> committedLog, LogOrderingChecker checker) {
        try {
            List<LogEntry> snapshot;
            synchronized (committedLog) {
                snapshot = new ArrayList<>(committedLog);
            }
            LogOrderingChecker.OrderingReport report = checker.check(snapshot);
            NodeLog.info(nodeId, "ORDER CHECK entries=" + snapshot.size()
                    + " lamportErrors=" + report.lamportErrors().size()
                    + " wallClockRegressions=" + report.wallClockRegressions().size()
                    + " missingTimestamps=" + report.missingTimestamps().size());
            report.lamportErrors().stream().limit(3).forEach(m -> NodeLog.info(nodeId, "ORDER CHECK " + m));
            report.wallClockRegressions().stream().limit(3).forEach(m -> NodeLog.info(nodeId, "ORDER CHECK " + m));
        } catch (Exception e) {
            NodeLog.info(nodeId, "ORDER CHECK failed: " + e.getMessage());
        }
    }

    /** Reads arguments of the form: --name value --name value */
    private static Map<String, String> parseOptions(String[] args) {
        Map<String, String> options = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            if (args[i].startsWith("--")) {
                options.put(args[i].substring(2), args[i + 1]);
            }
        }
        return options;
    }
}
