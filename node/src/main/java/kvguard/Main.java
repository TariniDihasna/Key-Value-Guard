package kvguard;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

        FailureDetector failureDetector = new FailureDetector(nodeId, peerAddresses, faults);
        failureDetector.start();
        ConsensusReplicator replicator = new ConsensusReplicator(store);
        ConsensusModule consensus = new ConsensusModule(
            nodeId,
            clusterPeers,
             "data/" + nodeId,
            replicator::onCommit, 
            faults);
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

        NodeLog.info(nodeId, "started on port " + port + ", peers=[" + peers + "]");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            NodeLog.info(nodeId, "shutting down");
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
