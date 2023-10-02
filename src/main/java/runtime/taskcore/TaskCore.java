package runtime.taskcore;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class TaskCore extends Thread {
    private static final Logger log = LoggerFactory.getLogger(TaskCore.class);

    private final TaskExecutor taskExecutor;

    private final IOManager ioManager;


    public TaskCore() {
        this.taskExecutor = new TaskExecutor();
        this.ioManager = new IOManager();
    }

    @Override
    public void run() {
        log.info("Starting");
        ioManager.subscribeConsumer();

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

        records = ioManager.pollRequests(Duration.ZERO);

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
     // TODO: add lightweight test cases
    }
}
