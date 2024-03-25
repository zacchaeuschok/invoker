package runtime;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import runtime.KafkaIOManager;


public class Application extends Thread {
    private static final Logger log = LoggerFactory.getLogger(runtime.Application.class);

    private final KafkaPollNettyServer socket;

    private runtime.KafkaIOManager kafkaIoManager;

    private SharedStatus configStatus;

    private ChannelGroup allClients;


    public Application(SharedStatus configStatus) {
        this.kafkaIoManager = new KafkaIOManager();
        this.configStatus = configStatus;
        this.allClients = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
        this.socket = new KafkaPollNettyServer(50001, this.allClients);
    }

    @Override
    public void run() {
        System.out.println("Starting");
        //kafkaIoManager.subscribeConsumer();
        socket.start();

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
            System.out.println("Received records");
            records.forEach(record -> {
                byte[] key = record.key();
                byte[] value = record.value();

                if (key != null && value != null) {
                    ByteBuf encodedMsg = Unpooled.buffer();
                    encodedMsg.writeInt(key.length);
                    encodedMsg.writeBytes(key);
                    encodedMsg.writeInt(value.length);
                    encodedMsg.writeBytes(value);
                    allClients.write(encodedMsg);
                }
            });
            allClients.flush();
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
