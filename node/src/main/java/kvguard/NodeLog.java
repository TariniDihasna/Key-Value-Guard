package kvguard;

import java.time.Instant;

public final class NodeLog {

    private NodeLog() { }

    public static void info(String nodeId, String message) {
        System.out.println(Instant.now() + " " + nodeId + " " + message);
    }
}
