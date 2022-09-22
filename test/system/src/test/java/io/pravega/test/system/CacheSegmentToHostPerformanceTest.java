/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.test.system;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;
import java.io.Serializable;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

import static org.apache.commons.lang.RandomStringUtils.randomAlphanumeric;
import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SystemTestRunner.class)
public class CacheSegmentToHostPerformanceTest extends AbstractReadWriteTest {
    private final static String STREAM_NAME = "testStreamSampleY";
    private final static String STREAM_SCOPE = "testScopeSampleY" + randomAlphanumeric(5);
    private final static String READER_GROUP = "ExampleReaderGroupY";
    private final static int NUM_EVENTS = 100;

    @Rule
    public Timeout globalTimeout = Timeout.seconds(5 * 60);

    private final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(4);
    private final StreamConfiguration config = StreamConfiguration.builder()
            .scalingPolicy(scalingPolicy)
            .build();

    /**
     * This is used to setup the various services required by the system test framework.
     */
    @Environment
    public static void initialize() {
        URI zkUri = startZookeeperInstance();
        startBookkeeperInstances(zkUri);
        URI controllerUri = ensureControllerRunning(zkUri);
        ensureSegmentStoreRunning(zkUri, controllerUri);
    }

    /**
     * Invoke the simpleTest, ensure we are able to produce  events.
     * The test fails incase of exceptions while writing to the stream.
     *
     */
    @Test
    public void simpleTest() {
        Service conService = Utils.createPravegaControllerService(null);
        List<URI> ctlURIs = conService.getServiceDetails();
        URI controllerUri = ctlURIs.get(0);

        log.info("Invoking create stream with Controller URI: {}", controllerUri);

        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(Utils.buildClientConfig(controllerUri));
        @Cleanup
        ControllerImpl controller = new ControllerImpl(ControllerImplConfig.builder()
                .clientConfig(Utils.buildClientConfig(controllerUri))
                .build(), connectionFactory.getInternalExecutor());

        assertTrue(controller.createScope(STREAM_SCOPE).join());
        assertTrue(controller.createStream(STREAM_SCOPE, STREAM_NAME, config).join());

        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(STREAM_SCOPE, Utils.buildClientConfig(controllerUri));
        log.info("Invoking Writer test with Controller URI: {}", controllerUri);

        @Cleanup
        EventStreamWriter<Serializable> writer = clientFactory.createEventWriter(STREAM_NAME,
                new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        for (int i = 0; i < 5; i++) {
            String event = "Publish " + i + "\n";
            log.debug("Producing event: {} ", event);
            // any exceptions while writing the event will fail the test.
            writer.writeEvent("", event);
            writer.flush();
        }

        log.info("Invoking performance test.");
        Segment seg = new Segment(STREAM_SCOPE, STREAM_NAME, 0);
        log.info("********START CACHE CALL*********** ");
        long start = System. currentTimeMillis();
        for (int i = 0; i < 500; i++) {
            try {
                CompletableFuture<PravegaNodeUri> uri = controller.getEndpointForSegment(seg.getScopedName());
            } catch (Exception e) {
                log.info("In exception of performance test");
            }
        }
        long elapsed = System. currentTimeMillis() - start;
        log.info("Elapsed time in millis for cache read call for cache----"+ elapsed +"--start time --"+ start+" -- end time --"+ System. currentTimeMillis());
        log.info("********END CACHE CALL*********** ");

        log.info("******************************************************************************************");

        log.info("Invoking performance test 2.....");
        Segment seg1 = new Segment(STREAM_SCOPE, "testStreamSampleY11", 1);
        log.info("********START N/W CALL*********** ");
        long start1 = System. currentTimeMillis();
        for (int i = 0; i < 500; i++) {
            try {
                CompletableFuture<PravegaNodeUri> uri = controller.getPravegaNodeUriForTest(seg1);
            } catch (Exception e) {
                log.info("In exception of performance test");
            }
        }
        long elapsed1 = System. currentTimeMillis() - start1;
        log.info("Elapsed time in millis for N/W read call for cache----"+ elapsed1 +"--start time --"+ start1+" -- end time --"+ System. currentTimeMillis());
        log.info("********END N/W CALL*********** ");
        
    }
}
