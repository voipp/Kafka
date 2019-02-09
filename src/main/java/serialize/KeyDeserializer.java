package serialize;

import com.fasterxml.jackson.databind.ObjectMapper;
import model.PurchaseKey;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class KeyDeserializer implements Deserializer<PurchaseKey> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public PurchaseKey deserialize(String topic, byte[] data) {
        ObjectMapper mapper = new ObjectMapper();
        PurchaseKey object = null;
        try {
            object = mapper.readValue(data, PurchaseKey.class);
        } catch (Exception exception) {
            System.out.println("Error in deserializing bytes " + exception);
        }
        return object;
    }

    @Override
    public void close() {
    }
}
