package runtime.taskcore;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runtime.taskcore.api.IOManager;

import java.time.Duration;
import java.util.List;

public class TaskCore extends Thread {
//public class TaskCore implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(TaskCore.class);

    private final TaskExecutor taskExecutor;

    private final IOManager socketIOManager;


    public TaskCore() {
        this.socketIOManager = new SocketIOManager();
        this.taskExecutor = new TaskExecutor(socketIOManager);
    }

    @Override
    public void run() {
        log.info("Starting");

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
        log.debug("Invoking poll on main Consumer");

        List<KeyValuePair> records = socketIOManager.pollRequests(Duration.ZERO);
        final int numRecords = records.size();

        log.debug("Main Consumer poll completed and fetched {} records",
                numRecords);

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
