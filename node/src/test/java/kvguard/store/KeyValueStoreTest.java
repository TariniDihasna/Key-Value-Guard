package kvguard.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import kvguard.store.KeyValueStore.ApplyResult;
import kvguard.store.KeyValueStore.Result;
import org.junit.jupiter.api.Test;


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
}
