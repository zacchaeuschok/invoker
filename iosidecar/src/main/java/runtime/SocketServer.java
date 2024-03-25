package runtime;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.newsclub.net.unix.AFUNIXServerSocket;
import org.newsclub.net.unix.AFUNIXSocketAddress;
import org.newsclub.net.unix.AFUNIXSocket;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;

public class SocketServer {
    private InputStream in;
    private OutputStream out;
    private AFUNIXServerSocket serverSocket;
    private AFUNIXSocket clientSocket;

    private static final int MAX_RETRIES = 10;
    private static final long RETRY_DELAY_MS = 1000;
    private static final File SOCKET_FILE = new File("/tmp/myapp.socket");

    public void start() {
        try {
            if (SOCKET_FILE.exists()) {
                SOCKET_FILE.delete();
            }
            serverSocket = AFUNIXServerSocket.newInstance();
            serverSocket.bind(new AFUNIXSocketAddress(SOCKET_FILE));
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
        in = clientSocket.getInputStream();
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
                        System.out.println("sent key " + new String(key, StandardCharsets.UTF_8));
                    } else {
                        out.write(intToBytes(0));
                    }
                    byte[] value = record.value();
                    if (value != null) {
                        out.write(intToBytes(value.length)); // Send the length of the value
                        out.write(value); // Send the value itself
                        System.out.println("sent value " + new String(value, StandardCharsets.UTF_8));
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

    public InputStream getIn() {
        return this.in;
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
