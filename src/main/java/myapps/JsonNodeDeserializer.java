package myapps;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

/**
 * The JsonNodeDeserializer class is a Kafka Deserializer implementation for deserializing JSON data
 * into Jackson's JsonNode objects.
 */
public class JsonNodeDeserializer implements Deserializer<JsonNode> {

    /**
     * Configures the deserializer. No additional configuration is needed for this implementation.
     *
     * @param configs Configuration settings (not used).
     * @param isKey   Indicates whether the deserializer is for a key or value.
     */
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No additional configuration needed
    }

    /**
     * Deserializes the byte array representation of JSON data into a JsonNode object.
     *
     * @param topic The Kafka topic associated with the data (not used).
     * @param data  The byte array representing the serialized JSON data.
     * @return The deserialized JsonNode object or null if the input data is null.
     * @throws RuntimeException Thrown if there is an error during deserialization.
     */
    @Override
    public JsonNode deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;  // Handle null content gracefully
        }

        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readTree(data);
        } catch (IOException e) {
            throw new RuntimeException("Error deserializing JsonNode", e);
        }
    }

    /**
     * Closes the deserializer. This implementation has no resources to release.
     */
    @Override
    public void close() {
        // No resources to release
    }
}
