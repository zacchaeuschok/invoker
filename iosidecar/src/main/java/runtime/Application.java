package runtime;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import runtime.KafkaIOManager;


public class Application extends Thread {
    private static final Logger log = LoggerFactory.getLogger(runtime.Application.class);

    private final SocketServer socket;

    private final runtime.KafkaIOManager kafkaIoManager;


    public Application() {
        this.socket = new SocketServer();
        this.kafkaIoManager = new KafkaIOManager();
    }

    @Override
    public void run() {
        log.info("Starting");
        kafkaIoManager.subscribeConsumer();
        socket.start(5321);

        while(isRunning()) {
            runOnce();
        }
    }

    void runOnce() {
        if (isRunning()) {
            pollPhase();
        }
    }

    private void pollPhase() {
        final ConsumerRecords<byte[], byte[]> records;
        log.debug("Invoking poll on main Consumer");

        records = kafkaIoManager.pollRequests(Duration.ZERO);

        if (!records.isEmpty()) {
            socket.sendRecords(records);
        }
    }



    public boolean isRunning() {
        return true;
    }

    public static void main(String[] args) {
        Application app = new Application();
        Thread t1 =new Thread(app);
        t1.start();
    }
}
