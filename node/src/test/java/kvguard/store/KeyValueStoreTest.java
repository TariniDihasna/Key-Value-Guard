package kvguard.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import kvguard.store.KeyValueStore.ApplyResult;
import kvguard.store.KeyValueStore.Result;
import org.junit.jupiter.api.Test;
import kvguard.proto.LogEntry;
import kvguard.proto.Operation;

import java.security.Key;


class KeyValueStoreTest {
    @Test
    void casFromEmptyKeySucceeds() {
        KeyValueStore store = new KeyValueStore();

        ApplyResult result = store.cas("req1", "A12", "Tarini", 0);
        assertEquals(Result.OK,result.result());
        assertEquals(1,result.version());
        assertEquals("Tarini",store.get("A12").get().value());
    }

    @Test
    void SecondCasFromEmptyFails() {
        KeyValueStore store = new KeyValueStore();

        store.cas("req1","A12","Tarini",0);
        ApplyResult second = store.cas("req2","A12","Sisuri",0);

        assertEquals(Result.VERSION_MISMATCH,second.result());
        assertEquals("Tarini",store.get("A12").get().value());
    }

    @Test
    void retryWithSameRequestIdIsNotAppliedTwice() {
        KeyValueStore store = new KeyValueStore();

        store.cas("req1","A12","Tarini",0);
        ApplyResult retry = store.cas("req1","A12","Tarini",0);

        assertTrue(retry.duplicate());
        assertEquals(1,store.lastApplied());
    }

    @Test
    void applyingACasLogEntryWorks(){
        KeyValueStore store = new KeyValueStore();
        LogEntry entry = LogEntry.newBuilder().setRequestId("req1").setOp(Operation.OP_CAS).setKey("A12").setValue("Tarini").setExpectedVersion(0).build();
        
        assertEquals(Result.OK, store.apply(entry).result());
        assertEquals("Tarini",store.get("A12").get().value());
    }

    @Test
    void versionComesFromTheLogIndex() {
        KeyValueStore store = new KeyValueStore();
        LogEntry entry = LogEntry.newBuilder().setIndex(7).setRequestId("req1").setOp(Operation.OP_PUT).setKey("A12").setValue("Tarini").build();

        ApplyResult result = store.apply(entry);

        assertEquals(Result.OK,result.result());
        assertEquals(7,result.version());
        assertEquals(7,store.get("A12").get().version());
        assertEquals(7,store.lastApplied());
    }

    @Test
    void noopEntryDoesNotChangeDataButMovesLastApplied() {
        KeyValueStore store = new KeyValueStore();
        LogEntry noop = LogEntry.newBuilder().setIndex(3).setRequestId("raft-noop-node1-1").setOp(Operation.OP_NOOP).build();

        ApplyResult result = store.apply(noop);

        assertEquals(Result.OK,result.result());
        assertEquals(3,store.lastApplied());
        assertTrue(store.get("A12").isEmpty());
    }

    @Test
    void casUsesTheVersionFromTheLogIndex() {
        KeyValueStore store = new KeyValueStore();
        store.apply(LogEntry.newBuilder().setIndex(5).setRequestId("req1").setOp(Operation.OP_PUT).setKey("A12").setValue("Tarini").build());

        ApplyResult good = store.apply(LogEntry.newBuilder().setIndex(6).setRequestId("req2").setOp(Operation.OP_CAS).setKey("A12").setValue("Sisuri").setExpectedVersion(5).build());
        assertEquals(Result.OK,good.result());
        assertEquals(6,good.version());

        ApplyResult stale = store.apply(LogEntry.newBuilder().setIndex(7).setRequestId("req3").setOp(Operation.OP_CAS).setKey("A12").setValue("Shehani").setExpectedVersion(5).build());
        assertEquals(Result.VERSION_MISMATCH,stale.result());
        assertEquals("Sisuri",store.get("A12").get().value());
    }
}
