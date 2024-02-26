package runtime.taskcore;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class SocketClient {
    public static void main(String[] args) {
        String hostname = "localhost";
        int port = 5321;

        try (Socket socket = new Socket(hostname, port);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            String fromServer;
            while ((fromServer = in.readLine()) != null) {
                System.out.println("Received: " + fromServer);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

