package server;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class SocketServer {
    private ServerSocket serverSocket;
    private Socket clientSocket;
    private InputStream in;
    private KafkaIOManager ioManager;

    private static final int MAX_RETRIES = 10;
    private static final long RETRY_DELAY_MS = 1000;
    private SharedStatus configStatus = new SharedStatus();

    public static void main(String[] args) {
        SocketServer server = new SocketServer();
        server.start(50002);
    }

    public void start(int port) {
        try {
            ioManager = new KafkaIOManager();
            serverSocket = new ServerSocket(port);
            acceptClient();
            listen();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void acceptClient() throws IOException {
        System.out.println("Socket server started. Waiting for the main container to connect...");
        clientSocket = serverSocket.accept(); // Accepts connection from the main container
        System.out.println("Socket client accepted.");
        in = clientSocket.getInputStream();
    }

    private void listen() {
        // length of the message is provided in the first 4 bytes
        //Read Value
        Thread readerThread = new Thread(() -> {
            try {
                byte[] lengthBytes = new byte[4]; // length of the message is provided in the first 4 bytes
                while (!Thread.currentThread().isInterrupted()) {
                    System.out.println("Looping read");
                    in.read(lengthBytes);
                    int keyLength = bytesToInt(lengthBytes);
                    byte[] key = new byte[keyLength];
                    int keyRead = 0, keyTotalRead = 0;
                    while (keyTotalRead < keyLength && (keyRead = in.read(key, keyTotalRead, keyLength - keyTotalRead)) != -1) {
                        keyTotalRead += keyRead;
                    }

                    //Read Value
                    in.read(lengthBytes);
                    int valueLength = bytesToInt(lengthBytes);
                    byte[] value = new byte[valueLength];
                    int valueRead = 0, valueTotalRead = 0;
                    while (valueTotalRead < valueLength && (valueRead = in.read(value, valueTotalRead, valueLength - valueTotalRead)) != -1) {
                        valueTotalRead += valueRead;
                    }

                    String keyStr = new String(key, "UTF-8");
                    System.out.println("Received key: " + keyStr);
                    String valueStr = new String(value, "UTF-8");
                    System.out.println("Received value: " + valueStr);
                    if (configStatus.changed) {
                        ioManager = new KafkaIOManager();
                        configStatus.changed = false;
                    }
                    ioManager.send(keyStr, valueStr);
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


}

