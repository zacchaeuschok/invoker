package runtime.taskcore.api;

import runtime.taskcore.StateManager;
import sun.net.www.http.HttpClient;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.Socket;
import java.net.URL;

public class SimpleStateManager implements StateManager {
    private String urlString = "http://localhost:50000/data";
    private HttpURLConnection connection;

    private Socket socket;

    private PrintWriter writer;
    private String hostname = "localhost";
    private int port = 50000;
    private InputStream in;
    private OutputStream out;
    private boolean reconnecting = false;
    private final int maxRetries = 5;

    private final long waitTimeInMillis = 10000;

    public SimpleStateManager() {
        connect();
    }

    public void connect() {
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                this.socket = new Socket(hostname, port);
                this.out = socket.getOutputStream();
                this.in = socket.getInputStream();
                this.writer = new PrintWriter(this.out, true);
                System.out.println("State Connection established");
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

    @Override
    public void write(String key, String value) {
        try {
            String request = "POST " + key + "=" + value;
            writer.println(request);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String response = reader.readLine();
            System.out.println(response);
        } catch (Exception e) {
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
            e.printStackTrace();
        }
        return data;
    }

    public static void main(String[] args) {
        SimpleStateManager manager = new SimpleStateManager();
        manager.read("default");
    }
}
