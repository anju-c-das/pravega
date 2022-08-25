package io.pravega.client.control.impl;

import io.pravega.common.Timer;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import lombok.Data;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.concurrent.CompletableFuture;

/**
 * used to hold the endpoint details for the segment.
 *
 * Allows easy measurement of elapsed time. All elapsed time reported by this class is by reference to the time of
 * this object's creation.
 */
@Data
@RequiredArgsConstructor
public class CachedPravegaNodeUri {

    public static final int maxBackoffMillis = 20000; // TODO: to be moved to a common place, refer the same in EventWriter as well

    @NonNull
    private final Timer timer;

    @NonNull
    private CompletableFuture<PravegaNodeUri> pravegaNodeUri;

}
