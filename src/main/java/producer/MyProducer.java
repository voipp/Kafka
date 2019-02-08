package producer;

import model.PurchaseKey;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import partition.PurchaseKeyPartitioner;
import serialize.KeySerializer;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.Future;

public class MyProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "34.222.109.61:9092");
        props.put("key.serializer", KeySerializer.class.getName());
                props.put("value.serializer",
                        "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "1");
        props.put("retries", "1");
        props.put("request.timeout.ms", "100000");
        props.put("compression.type", "snappy");
        props.put("partitioner.class",
                PurchaseKeyPartitioner.class.getName());

        PurchaseKey key = new PurchaseKey("id1", new Date());
        String val = "{item:book, price:10.99}";

        try(KafkaProducer kafkaProducer = new KafkaProducer(props)){

            ProducerRecord<PurchaseKey, String> record = new ProducerRecord<>(
                    "first-topic1",
                    key,
                    val
                    );

            Callback callback = new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        System.out.println("Encountered exception " + e);
                    }
                }
            };

            Future<RecordMetadata> res = kafkaProducer.send(record, callback);

            kafkaProducer.flush();
        }
    }
}
