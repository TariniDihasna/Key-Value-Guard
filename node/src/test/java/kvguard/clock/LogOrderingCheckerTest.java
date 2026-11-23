
package kvguard.clock;

import kvguard.proto.LogEntry;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class LogOrderingCheckerTest {

    private final LogOrderingChecker checker =
            new LogOrderingChecker();

    private LogEntry entry(long index, long lamport, long wallTime) {
        return LogEntry.newBuilder()
                .setIndex(index)
                .setLamport(lamport)
                .setWallTimeMs(wallTime)
                .build();
    }

    @Test
    void detectsCorrectLamportOrdering() {
        var report = checker.check(List.of(
                entry(1, 5, 1000),
                entry(2, 8, 2000),
                entry(3, 10, 3000)
        ));

        assertFalse(report.hasLamportErrors());
        assertTrue(report.isValid());
    }

    @Test
    void detectsLamportOrderingError() {
        var report = checker.check(List.of(
                entry(1, 8, 1000),
                entry(2, 6, 2000),
                entry(3, 10, 3000)
        ));

        assertTrue(report.hasLamportErrors());
    }

    @Test
    void detectsWallClockRegression() {
        var report = checker.check(List.of(
                entry(1, 5, 3000),
                entry(2, 8, 2000)
        ));

        assertTrue(report.hasWallClockRegressions());
    }

    @Test
    void detectsMissingTimestamps() {
        var report = checker.check(List.of(
                entry(1, 0, 0)
        ));

        assertFalse(report.isValid());
        assertEquals(2, report.missingTimestamps().size());
    }
}
