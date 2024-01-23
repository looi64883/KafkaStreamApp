package myapps;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KafkaApp {

    public static void main(String[] args) throws Exception {

        final Logger logger = LoggerFactory.getLogger(KafkaApp.class);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-process-data-helloworld1");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "github-issues-tracker-hello");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // Use JSON serde for key and value
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsConfig streamsConfig = new StreamsConfig(props);

        StreamsBuilder builder = new StreamsBuilder();

        // Consume from github-issues-1 and github-issues-2 topics
        KStream<JsonNode, String> githubIssues1 = builder.stream("github-issues-1-looi1", Consumed.with(Serdes.serdeFrom(new JsonNodeSerializer(), new JsonNodeDeserializer()), Serdes.String()));
        KStream<JsonNode, String> githubIssues2 = builder.stream("github-issues-2-wan1", Consumed.with(Serdes.serdeFrom(new JsonNodeSerializer(), new JsonNodeDeserializer()), Serdes.String()));

        // Merge the two streams
        KStream<JsonNode, String> mergedStream = githubIssues1.merge(githubIssues2);

        // Extract "Body" and "Comment Body" values
        KStream<JsonNode, String> bodyAndCommentBodyStream = mergedStream
                .filter((key, value) -> {
                    // Log the raw data before parsing
                    logger.info("Raw JSON: {}", value);

                    // Use Jackson library to parse JSON
                    try {
                        ObjectMapper mapper = new ObjectMapper();
                        JsonNode jsonNode = mapper.readTree(value);

                        // Log the parsed JsonNode
                        logger.info("Parsed JSON: {}", jsonNode);

                        boolean hasBodyOrCommentBody = false;

                        if (jsonNode.has("records") && jsonNode.get("records").isArray()) {
                            // Iterate over the records array and check each record
                            for (JsonNode record : jsonNode.get("records")) {
                                if (record.isArray()) {
                                    // Iterate over the inner array
                                    for (JsonNode innerRecord : record) {
                                        if (innerRecord.has("Body") || innerRecord.has("Comment Body")) {
                                            hasBodyOrCommentBody = true;
                                            break;
                                        }
                                    }
                                }
                                if (hasBodyOrCommentBody) {
                                    break;
                                }
                            }
                        }

                        // Log whether the condition is true or false
                        if (hasBodyOrCommentBody) {
                            logger.info("JSON has 'Body' or 'Comment Body'");
                        } else {
                            logger.info("JSON does not have 'Body' or 'Comment Body'");
                        }

                        return hasBodyOrCommentBody;
                    } catch (Exception e) {
                        logger.error("Error parsing JSON: {}", value, e);
                        return false;
                    }
                })
                .mapValues(value -> {
                    try {
                        ObjectMapper mapper = new ObjectMapper();
                        JsonNode jsonNode = mapper.readTree(value);

                        // Log the parsed JsonNode
                        logger.info("Parsed JSON: {}", jsonNode);

                        List<String> bodyAndCommentBodyValues = new ArrayList<>();

                        if (jsonNode.has("records") && jsonNode.get("records").isArray()) {
                            // Iterate over the records array and collect "Body" and "Comment Body" values
                            for (JsonNode record : jsonNode.get("records")) {
                                if (record.isArray()) {
                                    // Iterate over the inner array
                                    for (JsonNode innerRecord : record) {
                                        if (innerRecord.has("Body")) {
                                            bodyAndCommentBodyValues.add(innerRecord.get("Body").asText());
                                        }
                                        if (innerRecord.has("Comment Body")) {
                                            bodyAndCommentBodyValues.add(innerRecord.get("Comment Body").asText());
                                        }
                                    }
                                }
                            }
                        }

                        // Log the collected values
                        logger.info("Collected Body and Comment Body values: {}", bodyAndCommentBodyValues);

                        // Join the values into a single string (you may adjust this part based on your requirements)
                        return String.join(" ", bodyAndCommentBodyValues);
                    } catch (Exception e) {
                        logger.error("Error parsing JSON: {}", value, e);
                        return "";
                    }
                });

        // Extract "Commenter" values
        KStream<JsonNode, String> commenterStream = mergedStream
                .filter((key, value) -> {
                    // Log the raw data before parsing
                    logger.info("Raw JSON: {}", value);

                    try {
                        ObjectMapper mapper = new ObjectMapper();
                        JsonNode jsonNode = mapper.readTree(value);

                        // Log the parsed JsonNode
                        logger.info("Parsed JSON: {}", jsonNode);

                        boolean hasCommenter = jsonNode.has("records") && jsonNode.get("records").isArray();

                        // Log whether the condition is true or false
                        if (hasCommenter) {
                            logger.info("JSON has 'records' array");
                        } else {
                            logger.info("JSON does not have 'records' array");
                        }

                        return hasCommenter;
                    } catch (Exception e) {
                        logger.error("Error parsing JSON: {}", value, e);
                        return false;
                    }
                })
                .mapValues(value -> {
                    try {
                        ObjectMapper mapper = new ObjectMapper();
                        JsonNode jsonNode = mapper.readTree(value);

                        List<String> commenterValues = new ArrayList<>();

                        if (jsonNode.has("records") && jsonNode.get("records").isArray()) {
                            // Iterate over the records array and collect "Commenter" values
                            for (JsonNode record : jsonNode.get("records")) {
                                if (record.isArray()) {
                                    // Iterate over the inner array
                                    for (JsonNode innerRecord : record) {
                                        if (innerRecord.has("Commenter")) {
                                            commenterValues.add(innerRecord.get("Commenter").asText());
                                        }
                                    }
                                }
                            }
                        }

                        // Log the collected values
                        logger.info("Collected Commenter values: {}", commenterValues);

                        // Join the values into a single string (you may adjust this part based on your requirements)
                        return String.join(" ", commenterValues);
                    } catch (Exception e) {
                        logger.error("Error parsing JSON: {}", value, e);
                        // Provide a default value or return null
                        return ""; // Or return null;
                    }
                });

        // Perform word count for Commenter values
        commenterStream
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\s")))
                .groupBy((key, word) -> word)
                .count()
                .toStream()
                .peek((key, value) -> {
                    // Log the content of streams-active-commenter-output
                    logger.info("streams-active-commenter-output - Key: {}, Value: {}", key, value);
                })
                .to("streams-active-commenter-output", Produced.with(Serdes.String(), Serdes.Long()));


        // Perform word count
        bodyAndCommentBodyStream
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                .groupBy((key, word) -> word)
                .count()
                .toStream()
                .peek((key, value) -> {
                    // Log the content of streams-active-commenter-output
                    logger.info("streams-wordcount-output - Key: {}, Value: {}", key, value);
                })
                .to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));

        final Topology topology = builder.build();

        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                // Stop Kafka Streams before cleaning up the state store
                streams.close();

                // Clean up and reset the state store after stopping Kafka Streams
                cleanAndResetStateStore(streams, streamsConfig);

                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    private static void cleanAndResetStateStore(KafkaStreams kafkaStreams, StreamsConfig streamsConfig) {
        try {
            // Stop Kafka Streams before cleaning up the state store
            kafkaStreams.close();

            // Get the application ID for creating the state store directory path
            String applicationId = streamsConfig.getString(StreamsConfig.APPLICATION_ID_CONFIG);

            // Get the state directory path
            String stateDirPath = streamsConfig.getString(StreamsConfig.STATE_DIR_CONFIG);

            // Construct the full path of the state store directory
            String stateStoreDirPath = stateDirPath + "/" + applicationId;

            // Check if the state store directory exists before attempting to delete
            File stateStoreDir = new File(stateStoreDirPath);
            if (stateStoreDir.exists()) {
                // Manually delete the state store directory and its contents
                deleteDirectory(stateStoreDir);
                System.out.println("State store directory deleted: " + stateStoreDirPath);
            } else {
                System.out.println("State store directory does not exist: " + stateStoreDirPath);
            }

            // Clean up and reset the state store after stopping Kafka Streams
            kafkaStreams.cleanUp();

        } catch (Exception e) {
            // Handle exceptions appropriately
            e.printStackTrace();
        }
    }


    private static void deleteDirectory(File directory) throws IOException {
        try {
            if (directory.exists()) {
                // Stop the Kafka broker before deleting the log directory
                // You need to stop the Kafka broker service or process

                // Then, proceed with deleting the log directory
                Files.walk(directory.toPath())
                        .map(java.nio.file.Path::toFile)
                        .sorted((o1, o2) -> -o1.compareTo(o2))
                        .forEach(File::delete);
                System.out.println("Directory deleted: " + directory.getAbsolutePath());
            } else {
                System.out.println("Directory does not exist: " + directory.getAbsolutePath());
            }
        } catch (Exception e) {
            // Handle exceptions appropriately
            e.printStackTrace();
        }
    }

}