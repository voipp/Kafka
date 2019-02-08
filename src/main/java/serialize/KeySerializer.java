package serialize;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import model.PurchaseKey;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * KeySerializer.
 *
 * @author Aleksey_Kuznetsov
 */
public class KeySerializer implements Serializer<PurchaseKey> {
    private final ObjectMapper mapper = new ObjectMapper();

    public KeySerializer() {
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, PurchaseKey data) {
        try {
            return mapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {

    }
}
