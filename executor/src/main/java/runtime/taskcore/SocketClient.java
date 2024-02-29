package runtime.taskcore;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.ByteBuffer;

public class SocketClient {
    public static void main(String[] args) {
        String hostname = "localhost";
        int port = 5321;

        try (Socket socket = new Socket(hostname, port);
             InputStream in = socket.getInputStream()) {
            System.out.println("Connected");
            byte[] lengthBytes = new byte[4];
            while (in.read(lengthBytes) != -1) { // Read the length of the next piece of data
                //Read Key
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
                
                String keyStr = new String(key, "UTF-8"); // Specify the correct charset if not UTF-8
                System.out.println("Received key: " + keyStr);
                String valueStr = new String(value, "UTF-8"); // Specify the correct charset if not UTF-8
                System.out.println("Received value: " + valueStr);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static int bytesToInt(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        return bb.getInt();
    }
}

