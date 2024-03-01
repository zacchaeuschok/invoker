package runtime.taskcore;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runtime.taskcore.api.SimpleStateManager;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;

public class TaskExecutor {
    private final Logger log = LoggerFactory.getLogger(TaskExecutor.class);

    private final ArrayDeque<KeyValuePair> fifoQueue;

    private final StateManager stateManager;
    private final int numIterations;

    private KeyValuePair record;

    public TaskExecutor() {
        this.stateManager = new SimpleStateManager();
        this.fifoQueue = new ArrayDeque<>();
        this.numIterations = 1;
    }


    int process() {
        int totalProcessed = 0;

        while (totalProcessed < numIterations) {
            if (fifoQueue.size() != 0) {
                System.out.println(fifoQueue.size());
            }
            if (!fifoQueue.isEmpty()) {
                record = fifoQueue.pollFirst();
                doProcess();
            }
            totalProcessed++;
        }

        return totalProcessed;
    }

    private void doProcess() {
        // TODO: define UDF to process the record
        String currData = stateManager.read("default");
        System.out.println("Do process" + record.value);
        String newData = currData + record.value;
        stateManager.write("default", newData);
        if (record != null) {
            System.out.println(record.value);
        }
    }


    /**
     * Adds records to queues. If a record has an invalid (i.e., negative) timestamp, the record is skipped
     * and not added to the queue for processing
     *
     * @param records   the records
     */
    public void addRecords(final Iterable<KeyValuePair> records) {
        addRawRecords(records);
    }

    private void addRawRecords(final Iterable<KeyValuePair> rawRecords) {
        System.out.println("Adding raw records");
        for (final KeyValuePair rawRecord : rawRecords) {
            fifoQueue.addLast(rawRecord);
        }
    }
}