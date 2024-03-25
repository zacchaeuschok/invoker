package runtime;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
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
        socket.start();
        listen(socket.getIn());

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

    private void listen(InputStream in) {
        Thread readerThread = new Thread(() -> {
            try {
                byte[] lengthBytes = new byte[4]; // length of the message is provided in the first 4 bytes
                while (!Thread.currentThread().isInterrupted()) {
                    System.out.println("Looping read");
                    in.read(lengthBytes);
                    int keyLength = bytesToInt(lengthBytes);
                    byte[] key = new byte[keyLength];
                    int keyRead = 0, keyTotalRead = 0;
                    while(keyTotalRead < keyLength && (keyRead = in.read(key, keyTotalRead, keyLength - keyTotalRead)) != -1) {
                        keyTotalRead += keyRead;
                    }

                    //Read Value
                    in.read(lengthBytes);
                    int valueLength = bytesToInt(lengthBytes);
                    byte[] value = new byte[valueLength];
                    int valueRead = 0, valueTotalRead = 0;
                    while(valueTotalRead < valueLength && (valueRead = in.read(value, valueTotalRead, valueLength - valueTotalRead)) != -1) {
                        valueTotalRead += valueRead;
                    }

                    String keyStr = new String(key, "UTF-8");
                    System.out.println("Received key: " + keyStr);
                    String valueStr = new String(value, "UTF-8");
                    System.out.println("Received value: " + valueStr);
                    if (configStatus.changed) {
                        kafkaIoManager = new KafkaIOManager();
                        configStatus.changed = false;
                    }
                    kafkaIoManager.send(keyStr, valueStr);
                }
            } catch (IOException e) {
                System.out.println("Error reading from socket: " + e);
            }
        });
        FileWatcher watcher = new FileWatcher("/etc/config", configStatus);
        Thread t1 =new Thread(watcher);
        t1.start();
        readerThread.start();
    }

    private int bytesToInt(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        return bb.getInt();
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
