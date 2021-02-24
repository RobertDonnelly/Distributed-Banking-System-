import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Application {

    private static final String TOPIC = "high-value-transactions";
    private static String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

    public static void main(String[] args) {
        //*****************
        // YOUR CODE HERE
        //*****************
        Application kafkaConsumerApp = new Application();

        String consumerGroup = "High-Value-Transaction";
        if (args.length == 1) {
            consumerGroup = args[0];
        }
        System.out.println("Consumer is part of the "+consumerGroup);

        Consumer<String,Transaction> kafkaConsumer =  kafkaConsumerApp.createKafkaConsumer(BOOTSTRAP_SERVERS, consumerGroup);

        kafkaConsumerApp.consumeMessages(TOPIC,kafkaConsumer);

    }

    public static void consumeMessages(String topic, Consumer<String, Transaction> kafkaConsumer) {
        //*****************
        // YOUR CODE HERE
        //*****************
        kafkaConsumer.subscribe(Collections.singletonList(topic));

        while (true){
            ConsumerRecords<String,Transaction> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));

            for(ConsumerRecord<String,Transaction> record: consumerRecords){
                System.out.printf("Received record with (key: %s, value: %s), partition: %d, offset: %d%n", record.key(),record.value(),record.partition(),record.offset());
                approveHighTransaction(record.value());
            }
            kafkaConsumer.commitAsync();
        }
    }

   public static Consumer<String, Transaction> createKafkaConsumer(String bootstrapServers, String consumerGroup) {
    //*****************
    // YOUR CODE HERE
    //*****************
       Properties properties = new Properties();

       properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
       properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
       properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Transaction.TransactionDeserializer.class.getName());
       properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
       properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

       return new KafkaConsumer<String, Transaction>(properties);
    }

    private static void approveHighTransaction(Transaction transaction) {
        // Print transaction information to the console

        //*****************
        // YOUR CODE HERE
        //*****************
        System.out.println(String.format("High Value Transaction: User = %s transaction amount = %.2f",
                transaction.getUser(),transaction.getAmount()));
    }

}
