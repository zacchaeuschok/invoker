package runtime.taskcore;

import runtime.taskcore.api.IOManager;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.newsclub.net.unix.AFUNIXSocket;
import org.newsclub.net.unix.AFUNIXSocketAddress;


public class SocketIOManager implements IOManager {

    private AFUNIXSocket ioSocket;
    private static final File SOCKET_FILE = new File("/tmp/myapp.socket");
    private String hostname = "localhost";
    private int inputPort = 50001;
    private InputStream in;
    private OutputStream out;
    private final int maxRetries = 5;

    private final long waitTimeInMillis = 10000;

    private int outputPort = 50002;

    private final Queue<KeyValuePair> messageQueue = new ConcurrentLinkedQueue<>();
    private Thread readerThread;

    private boolean reconnecting = false;

    public SocketIOManager() {
        try {
            this.ioSocket = AFUNIXSocket.newInstance();
        } catch (IOException e) {
            e.printStackTrace();
        }
        connectSocket();
        startReadingThread();
    }

    private void startReadingThread() {
        readerThread = new Thread(() -> {
            try {
                byte[] lengthBytes = new byte[4]; // length of the message is provided in the first 4 bytes
                while (!Thread.currentThread().isInterrupted()) {
                    System.out.println("Looping read");
                    if (in.read(lengthBytes) == -1) {
                        reconnecting = true;
                        // End of stream reached or socket closed
                    }
                    if (reconnecting) {
                        System.out.println("Reconnecting");
                        connectSocket();
                        continue;
                    }
                    // Read the length of the next piece of data
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
                    messageQueue.offer(new KeyValuePair(keyStr, valueStr));
                }
            } catch (IOException e) {
                System.out.println("Error reading from socket: " + e);
            }
        });
        readerThread.start();
    }

    public void connectSocket() {
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                ioSocket.connect(new AFUNIXSocketAddress(SOCKET_FILE));
                this.out = ioSocket.getOutputStream();
                this.in = ioSocket.getInputStream();
                System.out.println("I/O Connection established");
                reconnecting = false;
                break;
            } catch (IOException e) {
                System.out.println(e);
                if (attempt < maxRetries) {
                    try {
                        Thread.sleep(waitTimeInMillis);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        System.out.println("Thread interrupted: " + ie);
                        break;
                    }
                }
            }
        }
    }

    /**
     * Get the next batch of records by polling.
     *
     * @param pollTime how long to block in Consumer#poll
     * @return Next batch of records or null if no records available.
     */
    @Override
    public List<KeyValuePair> pollRequests(final Duration pollTime) {
        List<KeyValuePair> records = new ArrayList<>();
        while (!messageQueue.isEmpty()) {
            records.add(messageQueue.poll());
        }

        return records;
    }

    private int bytesToInt(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        return bb.getInt();
    }

    @Override
    public void send(KeyValuePair data) {
        try {
            byte[] key = data.key.getBytes("UTF-8");

            out.write(intToBytes(key.length)); // Send the length of the key
            if (key.length != 0) {
                out.write(key);
            }
            System.out.println("sent key: " + data.key);
            byte[] value = data.value.getBytes("UTF-8");
            out.write(intToBytes(value.length)); // Send the length of the value
            if (value.length != 0) {
                out.write(value);
            }
            System.out.println("sent val: " + data.value);

            out.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private byte[] intToBytes(int i) {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(i);
        return bb.array();
    }


}
