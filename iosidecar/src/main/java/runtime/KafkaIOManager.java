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
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

public class KafkaIOManager implements IOManager {

    private Consumer<byte[], byte[]> mainConsumer;
    private Producer<String, String> mainProducer;

    private String inputTopic = "test";
    private String outputTopic = "output";
    private String configBasePath = "/etc/config/";

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
        Map<String, String> configMapValues = ConfigMapReader.readAndParseConfigMap(configBasePath + "main");
        String input = configMapValues.get("input.topic");
        String output = configMapValues.get("output.topic");
        String kafkaBroker = configMapValues.get("kafka.broker");
        String podName = System.getenv("POD_NAME");
        if (kafkaBroker == null || kafkaBroker.isEmpty()) {
            kafkaBroker = "0.0.0.0:9092";
        }
        if (input != null && !input.isEmpty()) {
            inputTopic = input;
        }
        if (output != null && !output.isEmpty()) {
            outputTopic = output;
        }
        System.out.println(kafkaBroker);
        if (!isBrokerAddressValid(kafkaBroker)) {
            System.out.println("Invalid kafka broker address");
        }
        setupConsumer(kafkaBroker, podName);
        setupProducer(kafkaBroker);
        System.out.println("set up kafka io for topic " + inputTopic);
    }

    private void setupProducer(String kafkaBroker) {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.mainProducer = new KafkaProducer<>(props);
    }

    private void setupConsumer(String kafkaBroker, String podName) {
        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "group");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        this.mainConsumer = new KafkaConsumer<>(consumerProps);
        String partitionConfigPath = configBasePath + podName + ".partitions";
        List<String> assignedPartitions = ConfigMapReader.readAndParseList(partitionConfigPath);
        List<TopicPartition> assignedTPs = new ArrayList<>();
        for (String partition: assignedPartitions) {
            TopicPartition tp = new TopicPartition(inputTopic, Integer.parseInt(partition));
            assignedTPs.add(tp);
        }
        if (assignedTPs.isEmpty()) {
            subscribeConsumer();
        } else {
            this.mainConsumer.assign(assignedTPs);
        }
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
        mainProducer.close();
    }

    public void send(String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(outputTopic, key, value);
        mainProducer.send(record);
    }


    public void subscribeConsumer() {
        mainConsumer.subscribe(Collections.singletonList(inputTopic));
    }
}
