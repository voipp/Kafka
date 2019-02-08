package adminclient;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * AdminClientStarter.
 *
 * @author Aleksey_Kuznetsov
 */
public class AdminClientStarter {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "34.222.109.61:9092");
        properties.put("connections.max.idle.ms", 10000);
        properties.put("request.timeout.ms", 10000);
        try (AdminClient client = KafkaAdminClient.create(properties))
        {
            ListTopicsResult topics = client.listTopics();
            Set<String> names = topics.names().get();
            if (names.isEmpty())
            {
                // case: if no topic found.
            }
        }
        catch (InterruptedException | ExecutionException e)
        {
            // Kafka is not available
        }
    }
}
