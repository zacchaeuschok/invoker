package runtime;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.errors.TaskMigratedException;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaIOManager implements IOManager {

    private final Consumer<byte[], byte[]> mainConsumer;
    private String topic = "test";

    public KafkaIOManager() {
        final Properties props = new Properties();
        String kafkaBroker = System.getenv("KAFKA_BROKER");
        String kafkaTopic = System.getenv("INPUT_TOPIC");
        if (kafkaBroker == null || kafkaBroker.isEmpty()) {
            kafkaBroker = "host.docker.internal:9092";
        }
        if (kafkaTopic != null && !kafkaTopic.isEmpty()) {
            topic = kafkaTopic;
        }
        System.out.println(kafkaBroker);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        this.mainConsumer = new KafkaConsumer<>(props);
    }

    /**
     * Get the next batch of records by polling.
     *
     * @param pollTime how long to block in Consumer#poll
     * @return Next batch of records or null if no records available.
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    @Override
    public ConsumerRecords<byte[], byte[]> pollRequests(final Duration pollTime) {
        ConsumerRecords<byte[], byte[]> records = ConsumerRecords.empty();
        try {
            records = mainConsumer.poll(pollTime);
        } catch (final InvalidOffsetException e) {
            throw new RuntimeException(e);
        }

        return records;
    }

    public void close() {
        mainConsumer.close();
    }


    public void subscribeConsumer() {
        mainConsumer.subscribe(Collections.singletonList(topic));
    }
}
