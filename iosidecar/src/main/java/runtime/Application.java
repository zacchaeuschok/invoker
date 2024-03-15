package runtime;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import runtime.KafkaIOManager;


public class Application extends Thread {
    private static final Logger log = LoggerFactory.getLogger(runtime.Application.class);

    private final SocketServer socket;

    private runtime.KafkaIOManager kafkaIoManager;

    private SharedStatus configStatus;


    public Application(SharedStatus configStatus) {
        this.socket = new SocketServer();
        this.kafkaIoManager = new KafkaIOManager();
        this.configStatus = configStatus;
    }

    @Override
    public void run() {
        log.info("Starting");
        //kafkaIoManager.subscribeConsumer();
        socket.start(50001);

        while(isRunning()) {
            runOnce();
        }
    }

    void runOnce() {
        if (isRunning()) {
            if (configStatus.changed) {
                kafkaIoManager = new KafkaIOManager();
                configStatus.changed = false;
            }
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
        SharedStatus changedConfig = new SharedStatus();
        Application app = new Application(changedConfig);
        FileWatcher watcher = new FileWatcher("/etc/config", changedConfig);
        Thread t1 =new Thread(app);
        Thread t2 = new Thread(watcher);
        t1.start();
        t2.start();
    }
}
