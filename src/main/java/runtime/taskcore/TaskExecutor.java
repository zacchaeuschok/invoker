package runtime.taskcore;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static java.util.Collections.singleton;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.maybeMeasureLatency;

public class TaskExecutor {
    private final Logger log = LoggerFactory.getLogger(TaskExecutor.class);

    private final ArrayDeque<ConsumerRecord<byte[], byte[]>> fifoQueue;

    private final StateManager stateManager;
    private final int numIterations;

    private ConsumerRecord<byte[], byte[]> record;

    public TaskExecutor() {
        this.stateManager = new StateManager();
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
        // stateManager.read state
        // stateManager.write state
        System.out.println(record);
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
