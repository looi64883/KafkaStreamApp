package myapps;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class JsonNodeDeserializer implements Deserializer<JsonNode> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No additional configuration needed
    }

    @Override
    public JsonNode deserialize(String topic, byte[] data) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readTree(data);
        } catch (IOException e) {
            throw new RuntimeException("Error deserializing JsonNode", e);
        }
    }

    @Override
    public void close() {
        // No resources to release
    }
}

