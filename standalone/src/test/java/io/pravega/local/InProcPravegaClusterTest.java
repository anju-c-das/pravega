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
package io.pravega.local;

import io.pravega.test.common.SerializedClassRunner;
import lombok.extern.slf4j.Slf4j;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import static io.pravega.local.PravegaSanityTests.testWriteAndReadAnEvent;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.test.common.SerializedClassRunner;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;import java.util.UUID;
import java.util.concurrent.CompletableFuture; import static io.pravega.local.PravegaSanityTests.testWriteAndReadAnEvent;
import static org.junit.Assert.*;

/**
 * This class contains tests for in-process standalone cluster. It also configures and runs standalone mode cluster
 * with appropriate configuration for itself as well as for sub-classes.
 */
@Slf4j
@RunWith(SerializedClassRunner.class)
public class InProcPravegaClusterTest {

    @ClassRule
    public static final PravegaEmulatorResource EMULATOR = PravegaEmulatorResource.builder().build();
    final String msg = "Test message on the plaintext channel";

    /**
     * Compares reads and writes to verify that an in-process Pravega cluster responds properly with
     * with valid client configuration.
     *
     * Note:
     * Strictly speaking, this test is really an "integration test" and is a little time consuming. For now, its
     * intended to also run as a unit test, but it could be moved to an integration test suite if and when necessary.
     *
     */
    @Test(timeout = 50000)
    public void testWriteAndReadEventWithValidClientConfig() throws Exception {
        testWriteAndReadAnEvent("TestScope", "TestStream", msg, EMULATOR.getClientConfig());
    }

    @Test(timeout = 300000)
    public void testNullEvent() throws Exception {
                String scope = "TestScope";
                String stream = "TestStream";
                String message = msg;
                ClientConfig clientConfig = EMULATOR.getClientConfig();

                            int numSegments = 1;
                    @Cleanup
                    StreamManager streamManager = StreamManager.create(clientConfig);
                    streamManager.createScope(scope);
                    streamManager.createStream(scope, stream, StreamConfiguration.builder()
                                    .scalingPolicy(ScalingPolicy.fixed(numSegments))
                                    .build());
                    log.info("write an event");
                    @Cleanup
                   EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

                            // Write an event to the stream.
                                    @Cleanup
                    EventStreamWriter<String> writer = clientFactory.createEventWriter(stream,
                                    new JavaSerializer<String>(),
                                    EventWriterConfig.builder().build());
                    log.debug("Done writing message '{}' to stream '{} / {}'", message, scope, stream);

                            // Now, read the event from the stream.
                                   String readerGroup = UUID.randomUUID().toString().replace("-", "");
                    ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                                    .stream(Stream.of(scope, stream))
                                    .disableAutomaticCheckpoints()
                                   .build();

                @Cleanup
                   ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, clientConfig);
                   readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);

                           @Cleanup
                   ReaderGroup readerGroup1 = readerGroupManager.getReaderGroup(readerGroup);


                             @Cleanup
                    EventStreamReader<String> reader = clientFactory.createReader(
                                   "readerId", readerGroup,
                                   new JavaSerializer<String>(), ReaderConfig.builder().initialAllocationDelay(0).build());

                        for (int i = 0; i < 10000; i++) {
                        writeData(writer, String.valueOf(i));
                       // writer.flush();
                              long startTime =  System.currentTimeMillis();
                        //long unreadBytes = readerGroup1.getMetrics().unreadBytes();
                    System.out.println("@@@@@@"+ readData(reader)+ "elapsed Time :: "+ (System.currentTimeMillis() - startTime));
                    }
              }

            private static void writeData(EventStreamWriter<String> writer, String event) {
                final CompletableFuture<Void> writeFuture = writer.writeEvent(event);
                writeFuture.join();
           }

            private static String readData(EventStreamReader<String> reader) {
                EventRead<String> eventRead = reader.readNextEvent(1000);
                return eventRead.getEvent();
           }
}
