package kvguard.failure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test; 

public class FailureDetectorTest {
    @Test 
    void peerIsSuspectedAfterthreeHeartbeatFailures(){
        peerHealth peer = new peerHealth("node2");
        peer.recordFailure(3);
        peer.recordFailure(3);

        assertEquals(peerHealth.State.UNKNOWN, peer.getState());

        peer.recordFailure(3);

        assertEquals(peerHealth.State.SUSPECTED, peer.getState());
        assertEquals(3, peer.getConsecutiveFailures());
    }

    @Test 
    void peerRecoversAfterSuccessfulHeartbeat(){
        peerHealth peer = new peerHealth("node2");
        peer.recordFailure(3);
        peer.recordFailure(3);
        peer.recordFailure(3);

        boolean recovered = peer.recordSuccess(1000);
        assertTrue(recovered);
        assertEquals(peerHealth.State.ALIVE, peer.getState());
        assertEquals(0, peer.getConsecutiveFailures());
        assertEquals(1000, peer.getLastSuccessfulHeartbeatMs());

    }

    @Test 
    void successfulHeartbeatResetsFailureCount(){
        peerHealth peer = new peerHealth("node2");
        
        peer.recordFailure(3);
        peer.recordFailure(3);

        peer.recordSuccess(2000);

        assertEquals(peerHealth.State.ALIVE, peer.getState());
        assertEquals(0, peer.getConsecutiveFailures());

        peer.recordFailure(3);
        assertEquals(1, peer.getConsecutiveFailures());
        assertEquals(peerHealth.State.ALIVE, peer.getState());
    }

    @Test 
    void blockedPeerIsDetectedAsDropped(){
        FaultManager faults = new FaultManager();
        kvguard.proto.FaultConfig config =
                     kvguard.proto.FaultConfig.newBuilder()
                                   .addBlockedPeers("node2")
                                   .build();
        faults.configure(config);
        assertTrue(faults.isBlocked("node2")); 
        assertTrue(faults.shouldDrop("node2")); 
        assertFalse(faults.isBlocked("node3"));     
    }

    @Test
    void clearAllRemovesFaultConfiguration() {
    FaultManager faults = new FaultManager();

    kvguard.proto.FaultConfig config =
            kvguard.proto.FaultConfig.newBuilder()
                    .addBlockedPeers("node2")
                    .setDelayMs(100)
                    .setDropProbability(0.5)
                    .setClockSkewMs(2000)
                    .build();

    faults.configure(config);
    faults.clearAll();

    assertFalse(faults.isBlocked("node2"));
    assertEquals(0, faults.getDelayMs());
    assertEquals(0.0, faults.getDropProbability());
    assertEquals(0, faults.getClockSkewMs());
}
}
