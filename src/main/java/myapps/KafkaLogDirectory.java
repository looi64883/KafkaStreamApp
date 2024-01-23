package myapps;

import java.io.FileInputStream;
import java.util.Properties;

public class KafkaLogDirectory {

    public static void main(String[] args) {
        // Provide the path to your server.properties file
        String serverPropertiesPath = "C:/Kafka/config/server.properties";

        try {
            // Load the server.properties file
            Properties properties = new Properties();
            properties.load(new FileInputStream(serverPropertiesPath));

            // Get the log directory path
            String logDirectoryPath = properties.getProperty("log.dirs");

            if (logDirectoryPath != null) {
                System.out.println("Log directory path: " + logDirectoryPath);
                // Use logDirectoryPath as needed
            } else {
                System.out.println("Log directory path not found in server.properties.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

