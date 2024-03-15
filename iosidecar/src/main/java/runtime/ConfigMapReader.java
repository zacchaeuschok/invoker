package runtime;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static java.util.Arrays.asList;

public class ConfigMapReader {

    public static Map<String, String> readAndParseConfigMap(String filePath) {
        Map<String, String> configValues = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.contains("=")) {
                    String[] parts = line.split("=", 2);
                    if (parts.length == 2) {
                        String key = parts[0].trim();
                        String value = parts[1].trim();
                        configValues.put(key, value);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return configValues;
    }

    public static List<String> readAndParseList(String filePath) {
        List<String> configValues = new ArrayList<>();
        try {
            String content = new String(Files.readAllBytes(Paths.get(filePath)));
            configValues = Arrays.asList(content.split(","));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return configValues;
    }
}
