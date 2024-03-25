import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.InvalidParameterException;
import java.util.HashMap;
import org.newsclub.net.unix.AFUNIXServerSocket;
import org.newsclub.net.unix.AFUNIXSocketAddress;
import org.newsclub.net.unix.AFUNIXSocket;

public class StateServer {

    private static final HashMap<String, String> storage = new HashMap<>();
    private static final File SOCKET_FILE = new File("/tmp/storage.socket");

    public static void main(String[] args) throws IOException {
        storage.put("default", "");
        System.out.println("Server started at " + SOCKET_FILE.getPath());

        try (AFUNIXServerSocket serverSocket = AFUNIXServerSocket.newInstance();) {
            serverSocket.bind(new AFUNIXSocketAddress(SOCKET_FILE));
            System.out.println("Server is listening on port " + SOCKET_FILE.getPath());
            while (true) {
                try (AFUNIXSocket socket = serverSocket.accept()) {
                    System.out.println("New client connected");

                    InputStream input = socket.getInputStream();
                    BufferedReader reader = new BufferedReader(new InputStreamReader(input));
                    OutputStream output = socket.getOutputStream();
                    PrintWriter writer = new PrintWriter(output, true);

                    String text;
                    while ((text = reader.readLine()) != null) {
                        System.out.println("Message from client: " + text);
                        String response = handleRequest(text);
                        writer.println(response);
                    }

                    System.out.println("Client disconnected.");
                } catch (IOException ex) {
                    System.out.println("Connection exception: " + ex.getMessage());
                    ex.printStackTrace();
                }
            }
        } catch (IOException ex) {
            System.out.println("Server exception: " + ex.getMessage());
            ex.printStackTrace();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String handleRequest(String text) {
        try {
            if (text.startsWith("GET")) {
                KeyValuePair kv = parse(text.substring(4));
                if (storage.containsKey(kv.key)) {
                    return "200:" + storage.get(kv.key);
                }
                return "404:Key Not Found";
            } else if (text.startsWith("POST")) {
                KeyValuePair kv = parse(text.substring(5));
                storage.put(kv.key, kv.value);
                return "200:OK";
            } else {
                throw new InvalidParameterException();
            }
        } catch (InvalidParameterException e) {
            return "400:Bad Request";
        }
    }

    public static KeyValuePair parse(String str) throws InvalidParameterException {
        String[] parts = str.split("=", 2);
        if(parts.length == 2) {
            String key = parts[0];
            String value = parts[1];
            return new KeyValuePair(key, value);
        }
        throw new InvalidParameterException();
    }



}
