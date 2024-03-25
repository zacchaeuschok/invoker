package runtime.taskcore.api;

import runtime.taskcore.StateManager;
import org.newsclub.net.unix.AFUNIXSocket;
import org.newsclub.net.unix.AFUNIXSocketAddress;

import java.io.*;

public class SimpleStateManager implements StateManager {
    private AFUNIXSocket socket;

    private PrintWriter writer;
    private static final File SOCKET_FILE = new File("/tmp/storage.socket");

    private InputStream in;
    private OutputStream out;
    private final int maxRetries = 5;

    private final long waitTimeInMillis = 10000;

    public SimpleStateManager() {
        connect();
    }

    public void connect() {
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                this.socket = AFUNIXSocket.newInstance();
                this.socket.connect(new AFUNIXSocketAddress(SOCKET_FILE));
                this.out = socket.getOutputStream();
                this.in = socket.getInputStream();
                this.writer = new PrintWriter(this.out, true);
                System.out.println("State Connection established");
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

    @Override
    public void write(String key, String value) {
        try {
            String request = "POST " + key + "=" + value;
            writer.println(request);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String response = reader.readLine();
            System.out.println(response);
        } catch (Exception e) {
            connect();
            System.out.println(e);
        }
    }
    @Override
    public String read(String key) {
        String data = "";
        try {
            String request = "GET " + key + "=";
            writer.println(request);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String response = reader.readLine();
            System.out.println(response);
            if (response.startsWith("200:")) {
                data = response.substring(4);
            }
        } catch (Exception e) {
            connect();
            e.printStackTrace();
        }
        return data;
    }

    public static void main(String[] args) {
        SimpleStateManager manager = new SimpleStateManager();
        manager.read("default");
    }
}
