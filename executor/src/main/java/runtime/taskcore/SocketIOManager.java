package runtime.taskcore;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.TaskMigratedException;
import runtime.taskcore.api.IOManager;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.Socket;
import java.net.URL;
import java.nio.ByteBuffer;
import java.security.Key;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;


public class SocketIOManager implements IOManager {

    private Socket socket;

    private String hostname = "localhost";
    private int port = 50001;
    private InputStream in;
    private String topic = "test";
    private final int maxRetries = 5;

    private final long waitTimeInMillis = 10000;

    private String producerURL = "http://localhost:50002/data";

    private final Queue<KeyValuePair> messageQueue = new ConcurrentLinkedQueue<>();
    private Thread readerThread;

    private boolean reconnecting = false;

    public SocketIOManager() {
        connect();
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
                        connect();
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

    public void connect() {
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                this.socket = new Socket(hostname, port);
                this.in = socket.getInputStream();
                System.out.println("Connection established");
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
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
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
            URL url = new URL(producerURL + "?key=" + data.key);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");

            connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            connection.setDoOutput(true); // Enable sending a request body

            // Sending request data
            try (OutputStream os = connection.getOutputStream()) {
                byte[] input = data.value.getBytes("utf-8");
                os.write(input, 0, input.length);
            }

            int responseCode = connection.getResponseCode();
            System.out.println("Response Code: " + responseCode);

            try (InputStream stream = connection.getInputStream();
                 InputStreamReader inputStreamReader = new InputStreamReader(stream);
                 BufferedReader reader = new BufferedReader(inputStreamReader)) {

                String line;
                StringBuilder response = new StringBuilder();

                while ((line = reader.readLine()) != null) {
                    response.append(line);
                }

                System.out.println(response.toString());
            }

            connection.disconnect();
        } catch (Exception e) {
            System.out.println(e);
        }
    }


}
