package kvguard.clock;

public interface ClockService {

    long lamportTick();

    long lamportOnReceive(long remote);

    long now();

    void setSkewMillis(long skew);

    void syncWith(String timeServerId);
}
