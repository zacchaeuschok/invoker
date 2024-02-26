package runtime.taskcore;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class TaskCore extends Thread {
//public class TaskCore implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(TaskCore.class);

    private final TaskExecutor taskExecutor;

    private final KafkaIOManager kafkaIoManager;


    public TaskCore() {
        this.taskExecutor = new TaskExecutor();
        this.kafkaIoManager = new KafkaIOManager();
    }

    @Override
    public void run() {
        log.info("Starting");
        kafkaIoManager.subscribeConsumer();

        while(isRunning()) {
            runOnce();
        }
    }

    void runOnce() {
        if (isRunning()) {
            pollPhase();
            taskExecutor.process();
        }
    }

    private void pollPhase() {
        final ConsumerRecords<byte[], byte[]> records;
        log.debug("Invoking poll on main Consumer");

        records = kafkaIoManager.pollRequests(Duration.ZERO);

        final int numRecords = records.count();

        log.debug("Main Consumer poll completed and fetched {} records from partitions {}",
                numRecords, records.partitions());

        if (!records.isEmpty()) {
            taskExecutor.addRecords(records);
        }
    }



    public boolean isRunning() {
        return true;
    }

    public static void main(String[] args) {
        TaskCore taskCore = new TaskCore();
        Thread t1 =new Thread(taskCore);
        t1.start();
    }
}
