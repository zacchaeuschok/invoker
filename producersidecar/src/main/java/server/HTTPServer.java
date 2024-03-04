package server;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class HTTPServer {

    private static KafkaIOManager ioManager;

    public static void main(String[] args) throws IOException {
        int port = 50002;
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        System.out.println("Server started at http://localhost:" + port);

        server.createContext("/data", new DataHandler());
        server.setExecutor(null); // creates a default executor
        server.start();
        ioManager = new KafkaIOManager();

    }

    static class DataHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                Map<String, String> queryParams = queryToMap(exchange.getRequestURI().getQuery());
                if ("POST".equals(exchange.getRequestMethod())) {
                    String response = handlePostRequest(exchange, queryParams);
                    sendResponse(exchange, 200, response);
                }
            } catch (Exception e) {
                sendResponse(exchange, 400, "Bad request");
            }
        }

        private String handlePostRequest(HttpExchange exchange, Map<String, String> queryParams) {
            StringBuilder requestBody = new StringBuilder();

            try (InputStream inputStream = exchange.getRequestBody();
                 InputStreamReader inputStreamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
                 BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {

                int b;
                while ((b = bufferedReader.read()) != -1) {
                    requestBody.append((char) b);
                }
                ioManager.send(queryParams.get("key"), requestBody.toString());

                return "POST request processed with data: " + requestBody.toString();
            } catch (IOException e) {
                // Handle exceptions or errors here
                e.printStackTrace();
                return "Error processing POST request";
            }
        }

        private Map<String, String> queryToMap(String query) {
            Map<String, String> result = new HashMap<>();
            if (query != null) {
                for (String param : query.split("&")) {
                    String[] entry = param.split("=");
                    if (entry.length > 1) {
                        result.put(entry[0], entry[1]);
                    } else {
                        result.put(entry[0], "");
                    }
                }
            }
            return result;
        }

        private void sendResponse(HttpExchange exchange, int statusCode, String response) throws IOException {
            exchange.sendResponseHeaders(statusCode, response.getBytes().length);
            OutputStream os = exchange.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }
    }
}
