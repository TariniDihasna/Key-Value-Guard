package kvguard;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;

public final class NodeLog {

    private static final Path LOG_DIR = Paths.get("logs");

    private NodeLog() { }

    public static synchronized void info(String nodeId, String message) {
        String line = Instant.now() + " " + nodeId + " " + message;
        System.out.println(line);
        try {
            Files.createDirectories(LOG_DIR);
            Files.writeString(LOG_DIR.resolve(nodeId + ".log"), line + System.lineSeparator(),
                    StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException ignored) {
            // logging to a file must never crash the node
        }
    }
}
