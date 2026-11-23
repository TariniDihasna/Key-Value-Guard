
package kvguard.clock;

import io.grpc.stub.StreamObserver;
import kvguard.proto.TimeRequest;
import kvguard.proto.TimeResponse;
import kvguard.proto.TimeServiceGrpc;


public class TimeServiceImpl
        extends TimeServiceGrpc.TimeServiceImplBase {

    private final ClockService clockService;

    public TimeServiceImpl(ClockService clockService) {
        this.clockService = clockService;
    }

    @Override
    public void getTime(
            TimeRequest request,
            StreamObserver<TimeResponse> responseObserver) {

        // Get this node's current time.
        long currentTime = clockService.now();

        // Generate a Lamport timestamp for the outgoing response.
        long lamportTimestamp = clockService.lamportTick();

        // Prepare the response.
        TimeResponse response = TimeResponse.newBuilder()
                .setServerTimeMs(currentTime)
                .setLamport(lamportTimestamp)
                .build();

        // Send the response to the requesting node.
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}

