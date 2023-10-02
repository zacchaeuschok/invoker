package runtime.taskcore;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.TaskMigratedException;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.streams.processor.internals.ClientUtils.getConsumerClientId;

public class IOManager {

    private final Consumer<byte[], byte[]> mainConsumer;
    public static final String TOPIC = "test";

    public IOManager() {
        final Properties props = new Properties();
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
        props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.putIfAbsent(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsConfig config = new StreamsConfig(props);
        final String applicationId = config.getString(StreamsConfig.APPLICATION_ID_CONFIG);
        final Long threadId = Thread.currentThread().getId();
        final int threadIdx = threadId.intValue();
        final String clientId = "Task-" + threadIdx;
        final Map<String, Object> consumerConfigs = config.getMainConsumerConfigs(applicationId, getConsumerClientId(clientId), threadIdx);

        this.mainConsumer = new KafkaConsumer<>(consumerConfigs, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    /**
     * Get the next batch of records by polling.
     *
     * @param pollTime how long to block in Consumer#poll
     * @return Next batch of records or null if no records available.
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    ConsumerRecords<byte[], byte[]> pollRequests(final Duration pollTime) {
        ConsumerRecords<byte[], byte[]> records = ConsumerRecords.empty();
        try {
            records = mainConsumer.poll(pollTime);
        } catch (final InvalidOffsetException e) {
            throw new RuntimeException(e);
        }

        return records;
    }

    void subscribeConsumer() {
        mainConsumer.subscribe(Collections.singletonList(TOPIC));
    }
}
