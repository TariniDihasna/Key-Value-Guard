
package kvguard.clock;

import kvguard.proto.LogEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Checks Lamport timestamp ordering and wall-clock
 * timestamp regressions in a sequence of log entries.
 */
public final class LogOrderingChecker {

    public record OrderingReport(
            List<String> lamportErrors,
            List<String> wallClockRegressions,
            List<String> missingTimestamps) {

        public OrderingReport {
            lamportErrors = List.copyOf(lamportErrors);
            wallClockRegressions = List.copyOf(wallClockRegressions);
            missingTimestamps = List.copyOf(missingTimestamps);
        }

        public boolean hasLamportErrors() {
            return !lamportErrors.isEmpty();
        }

        public boolean hasWallClockRegressions() {
            return !wallClockRegressions.isEmpty();
        }

        public boolean isValid() {
            return lamportErrors.isEmpty()
                    && missingTimestamps.isEmpty();
        }
    }

    /**
     * Checks entries in their existing log order.
     * Does not sort entries by timestamp.
     */
    public OrderingReport check(List<LogEntry> entries) {

        Objects.requireNonNull(entries, "entries cannot be null");

        List<String> lamportErrors = new ArrayList<>();
        List<String> wallClockRegressions = new ArrayList<>();
        List<String> missingTimestamps = new ArrayList<>();

        LogEntry previous = null;

        for (LogEntry current : entries) {

            if (current == null) {
                throw new IllegalArgumentException(
                        "Log entries cannot contain null values");
            }

            if (current.getLamport() <= 0) {
                missingTimestamps.add(
                        "Entry index " + current.getIndex()
                                + " has no valid Lamport timestamp");
            }

            if (current.getWallTimeMs() <= 0) {
                missingTimestamps.add(
                        "Entry index " + current.getIndex()
                                + " has no valid wall-clock timestamp");
            }

            if (previous != null) {

                if (previous.getLamport() > 0
                        && current.getLamport() > 0
                        && current.getLamport() <= previous.getLamport()) {

                    lamportErrors.add(
                            "Lamport ordering error between log entries "
                                    + previous.getIndex() + " and "
                                    + current.getIndex()
                                    + ": " + previous.getLamport()
                                    + " -> " + current.getLamport());
                }

                if (previous.getWallTimeMs() > 0
                        && current.getWallTimeMs() > 0
                        && current.getWallTimeMs()
                        < previous.getWallTimeMs()) {

                    wallClockRegressions.add(
                            "Wall-clock regression between log entries "
                                    + previous.getIndex() + " and "
                                    + current.getIndex()
                                    + ": " + previous.getWallTimeMs()
                                    + " -> " + current.getWallTimeMs());
                }
            }

            previous = current;
        }

        return new OrderingReport(
                lamportErrors,
                wallClockRegressions,
                missingTimestamps);
    }
}

