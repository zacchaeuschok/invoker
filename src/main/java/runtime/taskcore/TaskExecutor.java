package runtime.taskcore;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runtime.taskcore.api.SimpleStateManager;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;

public class TaskExecutor {
    private final Logger log = LoggerFactory.getLogger(TaskExecutor.class);

    private final ArrayDeque<ConsumerRecord<byte[], byte[]>> fifoQueue;

    private final StateManager stateManager;
    private final int numIterations;

    private ConsumerRecord<byte[], byte[]> record;

    public TaskExecutor() {
        this.stateManager = new SimpleStateManager();
        this.fifoQueue = new ArrayDeque<>();
        this.numIterations = 1;
    }


    int process() {
        int totalProcessed = 0;

        while (totalProcessed < numIterations) {
            record = fifoQueue.pollFirst();
            doProcess();
            totalProcessed++;
        }

        return totalProcessed;
    }

    private void doProcess() {
        // TODO: define UDF to process the record
        stateManager.read();
        stateManager.write();
        if (record != null) {
            String valueAsString = new String(record.value(), StandardCharsets.UTF_8);
            System.out.println(valueAsString);
        }
    }


    /**
     * Adds records to queues. If a record has an invalid (i.e., negative) timestamp, the record is skipped
     * and not added to the queue for processing
     *
     * @param records   the records
     */
    public void addRecords(final Iterable<ConsumerRecord<byte[], byte[]>> records) {
        addRawRecords(records);
    }

    private void addRawRecords(final Iterable<ConsumerRecord<byte[], byte[]>> rawRecords) {
        for (final ConsumerRecord<byte[], byte[]> rawRecord : rawRecords) {
            fifoQueue.addLast(rawRecord);
        }
    }
}
