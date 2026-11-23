package kvguard.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
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
                SubmitResponse r = submitWithRetry(channel,target,newRequest(clientId,Operation.OP_PUT).setKey(command.get(1)).setValue(command.get(2)).build());
                log(clientId,target,"PUT " + command.get(1), describe(r));
            }
            case "delete" -> {
                requireArgs(command, 2, "delete KEY");
                SubmitResponse r = submitWithRetry(channel,target,newRequest(clientId,Operation.OP_DELETE).setKey(command.get(1)).build());
                log(clientId,target,"DELETE " + command.get(1), describe(r));
            }
            case "cas" -> {
                requireArgs(command, 4, "cas KEY EXPECTED_VERSION VALUE");
                SubmitResponse r = submitWithRetry(channel,target,newRequest(clientId,Operation.OP_CAS).setKey(command.get(1)).setExpectedVersion(Long.parseLong(command.get(2))).setValue(command.get(3)).build());
                log(clientId,target,"CAS " + command.get(1) + " expected "  + command.get(2), describe(r));
            }
            case "get" -> {
                requireArgs(command, 2, "get KEY [stale]");
                boolean stale = command.size() > 2 && command.get(2).equalsIgnoreCase("stale");
                GetResponse r = getWithRetry(channel,target, GetRequest.newBuilder().setKey(command.get(1)).setMode(stale ? ReadMode.READ_STALE : ReadMode.READ_STRONG).build());
                log(clientId, target, "GET " + command.get(1), " status = " + r.getStatus() + " found = " + r.getFound() + " value = " + r.getValue() + " version = " + r.getVersion() + " applied = " + r.getAppliedIndex() + " servedBy= " + r.getServedBy());
            }
            case "ping" -> {
                PingResponse r = FailureDetectionServiceGrpc.newBlockingStub(channel)
                        .withDeadlineAfter(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                        .ping(PingRequest.newBuilder().setSenderId(clientId)
                                .setSentMs(System.currentTimeMillis()).build());
                log(clientId, target, "PING", "node = " + r.getNodeId() + " role= " + r.getRole()
                        + " term=" + r.getTerm());
            }
            default -> printUsage();
        }
    }

    private static final int MAX_ATTEMPTS = 5;

    private static final List<String> ALL_NODES = List.of("localhost:9001","localhost:9002","localhost:9003","localhost:9004","localhost:9005");

    private static String nextAddress(String current){
        int i = ALL_NODES.indexOf(current);
        return ALL_NODES.get((i+1)%ALL_NODES.size());
    }

    private static String addressOf(String nodeId) {
        return "localhost:900" + nodeId.replaceAll("\\D", "");
    }

    private static SubmitResponse submitWithRetry(ManagedChannel firstChannel,String firstTarget,SubmitRequest request) {
        ManagedChannel channel = firstChannel;
        String currentAddress = firstTarget;
        List<ManagedChannel> opened = new ArrayList<>();
        StatusRuntimeException lastError = null;
        SubmitResponse lastRespone = null;
        try {
            for(int attempt=1; attempt <= MAX_ATTEMPTS; attempt++) {
                try {
                    ClientServiceGrpc.ClientServiceBlockingStub api = ClientServiceGrpc.newBlockingStub(channel).withDeadlineAfter(TIMEOUT_SECONDS,TimeUnit.SECONDS);
                    SubmitResponse response = api.submit(request);
                    if(response.getStatus() != kvguard.proto.Status.NOT_LEADER){
                        return response;
                    }
                    lastRespone = response;
                    if(response.getLeaderHint().isEmpty()){
                        System.out.println(Instant.now() + " no leader known yet,waiting, request id = " + request.getRequestId());
                        Thread.sleep(500L * attempt);
                    }else{
                        String address = addressOf(response.getLeaderHint());
                        System.out.println(Instant.now() + " " + response.getServedBy() + " is not the leader,redirecting to " + response.getLeaderHint() + " (" + address + "), request id = " + request.getRequestId());
                        channel = ManagedChannelBuilder.forTarget(address).usePlaintext().build();
                        opened.add(channel);
                        currentAddress = address;
                    }
                }catch (StatusRuntimeException e) {
                    io.grpc.Status.Code code = e.getStatus().getCode();
                    boolean retryable = code == Status.Code.DEADLINE_EXCEEDED || code == Status.Code.UNAVAILABLE;
                    if(!retryable){
                        throw e;
                    }
                    lastError = e;
                    System.out.println(Instant.now() + " attempt " + attempt + " of " + MAX_ATTEMPTS + "failed (" + code + "), request id = " + request.getRequestId());
                    currentAddress = nextAddress(currentAddress);
                    channel = ManagedChannelBuilder.forTarget(currentAddress).usePlaintext().build();
                    opened.add(channel);
                    System.out.println(Instant.now() + " trying another node: " + currentAddress);
                    Thread.sleep(500L * attempt);
                }
            }
        }catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while retrying", ie);
        }finally {
            for (ManagedChannel c : opened){
                c.shutdownNow();
            }
        }
        if (lastRespone != null) {
            return lastRespone;
        }
        throw lastError;
    }

    private static GetResponse getWithRetry(ManagedChannel firstChannel, String firstTarget, GetRequest request) {
        ManagedChannel channel = firstChannel;
        String currentAddress = firstTarget;
        List<ManagedChannel> opened = new ArrayList<>();
        StatusRuntimeException lastError = null;
        try {
            for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
                try {
                    GetResponse response = ClientServiceGrpc.newBlockingStub(channel).withDeadlineAfter(TIMEOUT_SECONDS, TimeUnit.SECONDS).get(request);
                    if (response.getStatus() != kvguard.proto.Status.NOT_LEADER) {
                        return response;
                    }
                    if(response.getLeaderHint().isEmpty()){
                        System.out.println(Instant.now() + " no leader known yet,waiting");
                        Thread.sleep(500L * attempt);
                        continue;
                    }
                    currentAddress = addressOf(response.getLeaderHint());
                    System.out.println(Instant.now() + " " + response.getServedBy() + " is not the leader, redirecting to " + currentAddress);
                    channel = ManagedChannelBuilder.forTarget(currentAddress).usePlaintext().build();
                    opened.add(channel);
                } catch (StatusRuntimeException e) {
                    io.grpc.Status.Code code = e.getStatus().getCode();
                    if (code != io.grpc.Status.Code.DEADLINE_EXCEEDED && code != io.grpc.Status.Code.UNAVAILABLE) {
                        throw e;
                    }
                    lastError = e;
                    currentAddress = nextAddress(currentAddress);
                    System.out.println(Instant.now() + " attempt " + attempt + " failed (" + code + "), trying " + currentAddress);
                    channel = ManagedChannelBuilder.forTarget(currentAddress).usePlaintext().build();
                    opened.add(channel);
                    Thread.sleep(500L * attempt);
                }
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while retrying", ie);
        } finally {
            for (ManagedChannel c : opened) {
                c.shutdownNow();
            }
        }
        throw lastError != null ? lastError : new IllegalStateException("Could not get a reply");
    }

    private static SubmitRequest.Builder newRequest(String clientId, Operation op) {
        return SubmitRequest.newBuilder()
                .setRequestId(clientId + "-" + UUID.randomUUID())
                .setClientId(clientId)
                .setOp(op);
    }

    private static String describe(SubmitResponse r) {
        return "status = " + r.getStatus() + " version = " + r.getCurrentVersion()
                + " servedBy = " + r.getServedBy()
                + (r.getLeaderHint().isEmpty() ? "" : " leaderHint = " + r.getLeaderHint());
    }

    private static void requireArgs(List<String> command, int count, String usage) {
        if (command.size() < count) {
            throw new IllegalArgumentException("Usage: " + usage);
        }
    }

    private static void log(String clientId, String target, String what, String outcome) {
        System.out.println(Instant.now() + " client = " + clientId + " target = " + target
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
