package runtime.taskcore.api;

import runtime.taskcore.StateManager;
import sun.net.www.http.HttpClient;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

public class SimpleStateManager implements StateManager {
    private String urlString = "http://localhost:5500/data";
    private HttpURLConnection connection;

    @Override
    public void write(String key, String value) {
        try {
            URL url = new URL(urlString + "?key=" + key);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");

            connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            connection.setDoOutput(true); // Enable sending a request body

            // Sending request data
            String data = value;
            try (OutputStream os = connection.getOutputStream()) {
                byte[] input = data.getBytes("utf-8");
                os.write(input, 0, input.length);
            }

            int responseCode = connection.getResponseCode();
            System.out.println("Response Code: " + responseCode);

            try (InputStream stream = connection.getInputStream();
                 InputStreamReader inputStreamReader = new InputStreamReader(stream);
                 BufferedReader reader = new BufferedReader(inputStreamReader)) {

                String line;
                StringBuilder response = new StringBuilder();

                while ((line = reader.readLine()) != null) {
                    response.append(line);
                }

                System.out.println(response.toString());
            }

            connection.disconnect();
        } catch (Exception e) {
            System.out.println(e);
        }
    }
    @Override
    public String read(String key) {
        String data = "";
        try {
            URL url = new URL(urlString + "?key=" + key);
            System.out.println("OPening url conn");
            connection = (HttpURLConnection)url.openConnection();
            connection.setRequestMethod("GET");
            int responseCode = connection.getResponseCode();
            System.out.println("Response Code: " + responseCode);
            if (responseCode == 404) {
                return data;
            }

            try (InputStream stream = connection.getInputStream();
                 InputStreamReader inputStreamReader = new InputStreamReader(stream);
                 BufferedReader reader = new BufferedReader(inputStreamReader)) {

                String line;
                StringBuilder response = new StringBuilder();

                while ((line = reader.readLine()) != null) {
                    response.append(line);
                }
                System.out.println(response.toString());
                data = response.toString();
            }

            connection.disconnect();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return data;
    }

    public static void main(String[] args) {
        SimpleStateManager manager = new SimpleStateManager();
        manager.read("not exist");
    }
}
