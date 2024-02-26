package runtime;

import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;

public interface IOManager {
    ConsumerRecords<byte[], byte[]> pollRequests(final Duration pollTime);
}
