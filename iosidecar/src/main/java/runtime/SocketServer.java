package runtime;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

public class SocketServer {
    private ServerSocket serverSocket;
    private Socket clientSocket;
    private OutputStream out;

    private static final int MAX_RETRIES = 10;
    private static final long RETRY_DELAY_MS = 1000;

    public void start(int port) {
        try {
            serverSocket = new ServerSocket(port);
            acceptClient();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void acceptClient() throws IOException {
        System.out.println("Socket server started. Waiting for the main container to connect...");
        clientSocket = serverSocket.accept(); // Accepts connection from the main container
        System.out.println("Socket client accepted.");
        out = clientSocket.getOutputStream();
    }


    public void sendRecords(final Iterable<ConsumerRecord<byte[], byte[]>> records) {
        int attempt = 0;
        boolean sent = false;

        while (!sent && attempt < MAX_RETRIES) {
            try {
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    byte[] key = record.key();
                    if (key != null) {
                        out.write(intToBytes(key.length)); // Send the length of the value
                        out.write(key); // Send the value itself
                    }
                    System.out.println("sent key");
                    byte[] value = record.value();
                    if (value != null) {
                        out.write(intToBytes(value.length)); // Send the length of the value
                        out.write(value); // Send the value itself
                    }
                    out.flush();
                }
                sent = true;
            } catch (IOException e) {
                attempt++;
                System.out.println("Send failed, attempt " + attempt + " of " + MAX_RETRIES);
                try {
                    // Attempt to reconnect
                    reconnect();
                } catch (IOException reconnectException) {
                    System.out.println("Reconnect failed: " + reconnectException.getMessage());
                    if (attempt < MAX_RETRIES) {
                        try {
                            Thread.sleep(RETRY_DELAY_MS);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                }
            }
        }

        if (!sent) {
            System.out.println("Failed to send records after " + MAX_RETRIES + " attempts.");
        }
    }

    private void reconnect() throws IOException {
        // Close existing connections
        if (out != null) out.close();
        if (clientSocket != null && !clientSocket.isClosed()) clientSocket.close();

        acceptClient();
    }
    private byte[] intToBytes(int i) {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(i);
        return bb.array();
    }

    public void stop() {
        try {
            if (out != null) out.close();
            if (clientSocket != null) clientSocket.close();
            if (serverSocket != null) serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
