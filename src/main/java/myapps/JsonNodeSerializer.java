package myapps;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

/**
 * The JsonNodeSerializer class is a Kafka Serializer implementation for serializing Jackson's JsonNode objects
 * into byte arrays representing JSON data.
 */
public class JsonNodeSerializer implements Serializer<JsonNode> {

    /**
     * Configures the serializer. No additional configuration is needed for this implementation.
     *
     * @param configs Configuration settings (not used).
     * @param isKey   Indicates whether the serializer is for a key or value.
     */
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No additional configuration needed
    }

    /**
     * Serializes a JsonNode object into a byte array representation of JSON data.
     *
     * @param topic The Kafka topic associated with the data (not used).
     * @param data  The JsonNode object to be serialized.
     * @return The byte array representing the serialized JSON data.
     * @throws RuntimeException Thrown if there is an error during serialization.
     */
    @Override
    public byte[] serialize(String topic, JsonNode data) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error serializing JsonNode", e);
        }
    }

    /**
     * Closes the serializer. This implementation has no resources to release.
     */
    @Override
    public void close() {
        // No resources to release
    }
}