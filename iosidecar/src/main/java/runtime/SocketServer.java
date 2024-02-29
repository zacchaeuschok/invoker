package runtime;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class SocketServer {
    private ServerSocket serverSocket;
    private Socket clientSocket;
    private OutputStream out;

    public void start(int port) {
        try {
            serverSocket = new ServerSocket(port);
            System.out.println("Socket server started. Waiting for the main container to connect...");
            clientSocket = serverSocket.accept(); // Accepts connection from the main container
            System.out.println("Socket client accepted.");

            out = clientSocket.getOutputStream();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void sendRecords(final Iterable<ConsumerRecord<byte[], byte[]>> records) {
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
                // Flush to ensure the data is sent over the network
                out.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

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
