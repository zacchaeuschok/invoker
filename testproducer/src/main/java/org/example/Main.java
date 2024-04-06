package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class Main {
    public static volatile int frequency = 300;
    public static volatile boolean run = true;
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("partitioner.class", "org.apache.kafka.clients.producer.RoundRobinPartitioner");
        props.put("acks", "all");
        Thread scaleThread = new Thread(() -> {
            try {
                Runtime rt = Runtime.getRuntime();
                rt.exec("kubectl apply -f operator-test/testing_1.yaml");
                System.out.println("Applied testing_1");
                Thread.sleep(3*60*1000);
                frequency = 150;
                rt.exec("kubectl apply -f operator-test/testing_2.yaml");
                System.out.println("Applied testing_2");
                Thread.sleep(3*60*1000);
                frequency = 100;
                rt.exec("kubectl apply -f operator-test/testing_3.yaml");
                System.out.println("Applied testing_3");
                Thread.sleep(3*60*1000);
                frequency = 150;
                rt.exec("kubectl apply -f operator-test/testing_2.yaml");
                System.out.println("Applied testing_2");
                Thread.sleep(3*60*1000);
                frequency = 300;
                rt.exec("kubectl apply -f operator-test/testing_1.yaml");
                System.out.println("Applied testing_1");
                Thread.sleep(3*60*1000);
                rt.exec("kubectl delete -f operator-test/testing_1.yaml");
                System.out.println("Deleting testing_1");
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