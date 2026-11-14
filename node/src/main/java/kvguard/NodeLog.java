package kvguard;

import java.time.Instant;

/**
 * Tiny logging helper so every node prints lines in the same shape:
 *   2026-11-13T10:15:30.123Z node1 message
 * The checker tools (Role 1) and the report (all roles) read these lines, so keep the shape stable.
 * Add term, role and lamport to the line once those exist.
 */
public final class NodeLog {

    private NodeLog() { }

    public static void info(String nodeId, String message) {
        System.out.println(Instant.now() + " " + nodeId + " " + message);
    }
}
