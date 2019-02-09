package producer;

import model.PurchaseKey;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import serialize.KeyDeserializer;
import serialize.KeySerializer;

import java.util.Collections;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class MyProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(4);

        Future<?> consumerThread = executor.submit(() -> launchConsumer("group1", "consumer1"));
        Future<?> consumerThread2 = executor.submit(() -> launchConsumer("group1", "consumer2"));
        Future<?> consumerThread3 = executor.submit(() -> launchConsumer("group2", "consumerFromAnotherGroup"));
        Thread.sleep(1_000);
        Future<?> producerThread = executor.submit(() -> {
            try {
                launchProducer();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });

        executor.shutdown();

        consumerThread.get();
        consumerThread2.get();
        consumerThread3.get();
        producerThread.get();
    }

    private static void launchConsumer(String groupId, String name) {
        Properties props2 = getProperties();
        props2.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        Consumer<Long, String> consumer = new KafkaConsumer<>(props2);

        if (name.equals("consumer1")) {
            TopicPartition topicPartition = new TopicPartition("transactions", 0);
            consumer.assign(Collections.singletonList(topicPartition));
        } else if (name.equals("consumer2")) {
            TopicPartition topicPartition = new TopicPartition("transactions", 1);
            consumer.assign(Collections.singletonList(topicPartition));
        } else
            consumer.subscribe(Collections.singletonList("transactions"));

        for (int i = 0; i < 5; i++) {
            System.out.println("[Consumer]Starting consumer polling " + name);

            pollMessage(consumer, name);
        }

        consumer.close();
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KeyDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    private static void pollMessage(Consumer<Long, String> consumer, String name) {
        final ConsumerRecords<Long, String> rec = consumer.poll(5_000);

        rec.forEach(record -> {
            System.out.println("[Consumer] Received message from broker to consumer " + name
                    + " \nkey " + record.key()
                    + " value " + record.value()
                    + " partition " + record.partition()
                    + " offset " + record.offset());
        });

        consumer.commitAsync();
//        System.out.println("[Consumer]Consumer " + consumer + " after waiting for messages. Is empty?" + rec.isEmpty());
    }

    private static void launchProducer() throws InterruptedException, ExecutionException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KeySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
//        props.put(ProducerConfig.ACKS_CONFIG, "1");
//        props.put(ProducerConfig.RETRIES_CONFIG, "1");
//        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "100000");
//        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
//        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
//                PurchaseKeyPartitioner.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "client1");

        System.out.println("[Producer]Starting producer");

        try (KafkaProducer kafkaProducer = new KafkaProducer(props)) {

            for (int i = 1; i < 6; i++) {
                PurchaseKey key = new PurchaseKey("id" + i, new Date());
                String val = "{VALUE}" + i;

                ProducerRecord<PurchaseKey, String> record = new ProducerRecord<>(
                        "transactions",
                        i % 2,
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

                System.out.println("[Producer]Starting producer sending " + i);

                Future<RecordMetadata> res = kafkaProducer.send(record, callback);

                kafkaProducer.flush();

                System.out.println("[Producer]Producer record metadata: " + res.get().toString());
            }
        }
    }
}
