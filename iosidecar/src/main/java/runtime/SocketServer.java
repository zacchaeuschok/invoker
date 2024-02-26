package runtime;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class SocketServer {
    private ServerSocket serverSocket;
    private Socket clientSocket;
    private PrintWriter out;

    public void start(int port) {
        try {
            serverSocket = new ServerSocket(port);
            System.out.println("Socket server started. Waiting for the main container to connect...");
            clientSocket = serverSocket.accept(); // Accepts connection from the main container
            System.out.println("Socket client accepted.");

            out = new PrintWriter(clientSocket.getOutputStream(), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void sendRecords(final Iterable<ConsumerRecord<byte[], byte[]>> records) {
        for (final ConsumerRecord<byte[], byte[]> record : records) {
            String valueAsString = new String(record.value(), StandardCharsets.UTF_8);
            sendMessage(valueAsString);
        };
    }

    public void sendMessage(String message) {
        if (out != null) {
            out.println(message);
        }
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
