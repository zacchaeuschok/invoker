package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.Properties;

public class AutoProducer {
    public static volatile int frequency = 100;
    public static volatile boolean run = true;
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("partitioner.class", "org.apache.kafka.clients.producer.internals.DefaultPartitioner");
        props.put("acks", "all");
        Thread scaleThread = new Thread(() -> {
            try {
                Thread.sleep(3*60*1000);
                System.out.println("Stopping");
                run = false;
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        scaleThread.start();


        Producer<String, String> producer = new KafkaProducer<>(props);

        List<String> words = getWords();

        while (run) {
            int partition = 0;
            for (String word : words) {
                producer.send(new ProducerRecord<String, String>("input", partition,"", word));
                partition = (partition + 1) % 6;
                //System.out.println("Produced " + word);
                Thread.sleep(frequency); // Sleep for x second
            }
        }
        System.out.println("Closing Producer");
        producer.close();
    }

    private static List<String> getWords() {
        return SampleText.toFiveWordStrings();
    }
}