import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class Application {
    private static final List<String> TOPICS = Arrays.asList("valid-transactions", "suspicious-transactions");

    private static String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

    public static void main(String[] args) {
        //*****************
        // YOUR CODE HERE
        //*****************
        Application kafkaConsumerApp = new Application();

        String consumerGroup = "Reporting-Service";
        if (args.length == 1) {
            consumerGroup = args[0];
        }
        System.out.println("Consumer is part of the "+consumerGroup);

        Consumer<String, Transaction> kafkaConsumer = kafkaConsumerApp.createKafkaConsumer(BOOTSTRAP_SERVERS, consumerGroup);

        consumeMessages(TOPICS,kafkaConsumer);
    }

    public static KafkaConsumer<String, Transaction> consumeMessages(List<String> topics, Consumer<String, Transaction> kafkaConsumer) {
        //*****************
        // YOUR CODE HERE
        //*****************
        kafkaConsumer.subscribe(topics);

        while (true){
            ConsumerRecords<String,Transaction> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));

            if(consumerRecords.isEmpty()){
                //do stuff
            }

            for(ConsumerRecord<String,Transaction> record: consumerRecords){
                System.out.printf("Received record with (key: %s, value: %s, partition: %d, offset: %d%n",
                        record.key(),record.value(),record.partition(),record.offset());
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

    private static void recordTransactionForReporting(String topic, Transaction transaction) {
        // Print transaction information to the console
        // Print a different message depending on whether transaction is suspicious or valid

        //*****************
        // YOUR CODE HERE
        //*****************
        if(topic.equals("valid-transactions")) {
            System.out.println("Valid Transaction: " + transaction.toString());

        }
        else if(topic.equals("suspicious-transactions")){
            System.out.println("Suspicious Transaction: " + transaction.toString());

        }
    }

}

