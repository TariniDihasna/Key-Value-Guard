package kvguard;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import kvguard.store.KeyValueStore;

import java.util.HashMap;
import java.util.Map;

/**
 * Starts one node. Example:
 *   java -jar node.jar --id node1 --port 9001 --peers localhost:9001,localhost:9002,localhost:9003
 *
 * Every node is the same program, started with a different id and port.
 */
public class Main {

    public static void main(String[] args) throws Exception {
        Map<String, String> options = parseOptions(args);
        String nodeId = options.getOrDefault("id", "node1");
        int port = Integer.parseInt(options.getOrDefault("port", "9001"));
        String peers = options.getOrDefault("peers", "");

        KeyValueStore store = new KeyValueStore();

        Server server = ServerBuilder.forPort(port)
                .addService(new ClientServiceImpl(nodeId, store))
                .addService(new FailureDetectionServiceImpl(nodeId))
                .addService(new AdminServiceImpl(nodeId, store))
                // TODO (Role 2): add ConsensusService (RequestVote, AppendEntries)
                // TODO (Role 1): add TimeService (GetTime)
                .build()
                .start();

        NodeLog.info(nodeId, "started on port " + port + ", peers=[" + peers + "]");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            NodeLog.info(nodeId, "shutting down");
            server.shutdown();
        }));
        server.awaitTermination();
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
