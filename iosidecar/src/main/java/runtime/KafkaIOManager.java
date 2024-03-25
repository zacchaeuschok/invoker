package runtime;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.time.Duration;
import java.util.*;

public class KafkaIOManager implements IOManager {

    private final Consumer<byte[], byte[]> mainConsumer;
    private String topic = "test";

    public boolean isBrokerAddressValid(String brokerAddress) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddress);
        try (AdminClient adminClient = AdminClient.create(props)) {
            adminClient.listTopics().names().get();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public KafkaIOManager() {
        final Properties props = new Properties();
        String configBasePath = "/etc/config/";
        Map<String, String> configMapValues = ConfigMapReader.readAndParseConfigMap(configBasePath + "main");
        String kafkaTopic = configMapValues.get("input.topic");
        String kafkaBroker = configMapValues.get("kafka.broker");
        String podName = System.getenv("POD_NAME");
        if (kafkaBroker == null || kafkaBroker.isEmpty()) {
            kafkaBroker = "localhost:9092";
        }
        if (kafkaTopic != null && !kafkaTopic.isEmpty()) {
            topic = kafkaTopic;
        }
        System.out.println(kafkaBroker);
        if (!isBrokerAddressValid(kafkaBroker)) {
            System.out.println("Invalid kafka broker address");
        }
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        this.mainConsumer = new KafkaConsumer<>(props);
        String partitionConfigPath = configBasePath + podName + ".partitions";
        List<String> assignedPartitions = ConfigMapReader.readAndParseList(partitionConfigPath);
        List<TopicPartition> assignedTPs = new ArrayList<>();
        for (String partition: assignedPartitions) {
            TopicPartition tp = new TopicPartition(topic, Integer.parseInt(partition));
            assignedTPs.add(tp);
        }
        if (assignedTPs.isEmpty()) {
            subscribeConsumer();
        } else {
            this.mainConsumer.assign(assignedTPs);
        }
        System.out.println("set up kafka io for topic " + topic);
    }

    /**
     * Get the next batch of records by polling.
     *
     * @param pollTime how long to block in Consumer#poll
     * @return Next batch of records or null if no records available.
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
