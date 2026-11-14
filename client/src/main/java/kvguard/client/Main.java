package kvguard.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import kvguard.proto.ClientServiceGrpc;
import kvguard.proto.FailureDetectionServiceGrpc;
import kvguard.proto.GetRequest;
import kvguard.proto.GetResponse;
import kvguard.proto.Operation;
import kvguard.proto.PingRequest;
import kvguard.proto.PingResponse;
import kvguard.proto.ReadMode;
import kvguard.proto.SubmitRequest;
import kvguard.proto.SubmitResponse;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Command-line client. Every call prints one line with a timestamp, so the output can be saved
 * and analysed later (successful requests, errors, values read).
 *
 * Usage (options first, then the command):
 *   java -jar client.jar [--target localhost:9001] [--client c1] put KEY VALUE
 *   java -jar client.jar get KEY [stale]
 *   java -jar client.jar cas KEY EXPECTED_VERSION VALUE      (0 = key must not exist)
 *   java -jar client.jar delete KEY
 *   java -jar client.jar ping
 *
 * ROLE 4 next steps: retry on NOT_LEADER using leader_hint, reuse the SAME request id when
 * retrying, and add a mode that runs many requests from several concurrent clients.
 */
public class Main {

    private static final long TIMEOUT_SECONDS = 3;

    public static void main(String[] args) {
        String target = "localhost:9001";
        String clientId = "client1";
        List<String> command = new ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--target") && i + 1 < args.length) {
                target = args[++i];
            } else if (args[i].equals("--client") && i + 1 < args.length) {
                clientId = args[++i];
            } else {
                command.add(args[i]);
            }
        }
        if (command.isEmpty()) {
            printUsage();
            return;
        }

        ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        try {
            run(channel, target, clientId, command);
        } catch (StatusRuntimeException e) {
            log(clientId, target, command.get(0), "ERROR " + e.getStatus().getCode()
                    + " " + e.getStatus().getDescription());
        } finally {
            channel.shutdown();
            try {
                channel.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void run(ManagedChannel channel, String target, String clientId, List<String> command) {
        String name = command.get(0).toLowerCase();
        ClientServiceGrpc.ClientServiceBlockingStub api = ClientServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        switch (name) {
            case "put" -> {
                requireArgs(command, 3, "put KEY VALUE");
                SubmitResponse r = api.submit(newRequest(clientId, Operation.OP_PUT)
                        .setKey(command.get(1)).setValue(command.get(2)).build());
                log(clientId, target, "PUT " + command.get(1), describe(r));
            }
            case "delete" -> {
                requireArgs(command, 2, "delete KEY");
                SubmitResponse r = api.submit(newRequest(clientId, Operation.OP_DELETE)
                        .setKey(command.get(1)).build());
                log(clientId, target, "DELETE " + command.get(1), describe(r));
            }
            case "cas" -> {
                requireArgs(command, 4, "cas KEY EXPECTED_VERSION VALUE");
                SubmitResponse r = api.submit(newRequest(clientId, Operation.OP_CAS)
                        .setKey(command.get(1))
                        .setExpectedVersion(Long.parseLong(command.get(2)))
                        .setValue(command.get(3)).build());
                log(clientId, target, "CAS " + command.get(1) + " expected=" + command.get(2), describe(r));
            }
            case "get" -> {
                requireArgs(command, 2, "get KEY [stale]");
                boolean stale = command.size() > 2 && command.get(2).equalsIgnoreCase("stale");
                GetResponse r = api.get(GetRequest.newBuilder()
                        .setKey(command.get(1))
                        .setMode(stale ? ReadMode.READ_STALE : ReadMode.READ_STRONG)
                        .build());
                log(clientId, target, "GET " + command.get(1), "status=" + r.getStatus()
                        + " found=" + r.getFound() + " value=" + r.getValue()
                        + " version=" + r.getVersion() + " applied=" + r.getAppliedIndex()
                        + " servedBy=" + r.getServedBy());
            }
            case "ping" -> {
                PingResponse r = FailureDetectionServiceGrpc.newBlockingStub(channel)
                        .withDeadlineAfter(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                        .ping(PingRequest.newBuilder().setSenderId(clientId)
                                .setSentMs(System.currentTimeMillis()).build());
                log(clientId, target, "PING", "node=" + r.getNodeId() + " role=" + r.getRole()
                        + " term=" + r.getTerm());
            }
            default -> printUsage();
        }
    }

    private static SubmitRequest.Builder newRequest(String clientId, Operation op) {
        // A fresh request id per command. When retrying the SAME command, reuse the same id.
        return SubmitRequest.newBuilder()
                .setRequestId(clientId + "-" + UUID.randomUUID())
                .setClientId(clientId)
                .setOp(op);
    }

    private static String describe(SubmitResponse r) {
        return "status=" + r.getStatus() + " version=" + r.getCurrentVersion()
                + " servedBy=" + r.getServedBy()
                + (r.getLeaderHint().isEmpty() ? "" : " leaderHint=" + r.getLeaderHint());
    }

    private static void requireArgs(List<String> command, int count, String usage) {
        if (command.size() < count) {
            throw new IllegalArgumentException("Usage: " + usage);
        }
    }

    private static void log(String clientId, String target, String what, String outcome) {
        System.out.println(Instant.now() + " client=" + clientId + " target=" + target
                + " " + what + " -> " + outcome);
    }

    private static void printUsage() {
        System.out.println("Usage: java -jar client.jar [--target host:port] [--client id] COMMAND");
        System.out.println("  put KEY VALUE");
        System.out.println("  get KEY [stale]");
        System.out.println("  cas KEY EXPECTED_VERSION VALUE   (0 = key must not exist)");
        System.out.println("  delete KEY");
        System.out.println("  ping");
    }
}
