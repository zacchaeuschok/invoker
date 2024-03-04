package runtime.taskcore.api;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import runtime.taskcore.KeyValuePair;

import java.time.Duration;
import java.util.List;

public interface IOManager {
    List<KeyValuePair> pollRequests(Duration pollTime);

    void send(KeyValuePair data);
}
