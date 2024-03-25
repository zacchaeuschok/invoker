package server;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class KafkaIOManager {


    private final Producer<String, String> mainProducer;
    private String topic = "output";

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
        Map<String, String> configMapValues = ConfigMapReader.readAndParseConfigMap("/etc/config/main");
        String kafkaTopic = configMapValues.get("output.topic");
        String kafkaBroker = configMapValues.get("kafka.broker");
        final Properties props = new Properties();

        if (kafkaBroker == null || kafkaBroker.isEmpty()) {
            kafkaBroker = "localhost:9092";
        }
        if (kafkaTopic != null && !kafkaTopic.isEmpty()) {
            topic = kafkaTopic;
        };
        System.out.println(kafkaBroker);
        if (!isBrokerAddressValid(kafkaBroker)) {
            System.out.println("Invalid kafka broker address");
        }
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.mainProducer = new KafkaProducer<>(props);
    }


    public void send(String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        mainProducer.send(record);

    }

    public void close() {
        mainProducer.close();
    }


}
