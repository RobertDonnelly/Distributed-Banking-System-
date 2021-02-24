import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Banking API Service
 */
public class Application {
    private static final String TOPIC_A = "valid-transactions";
    private static final String TOPIC_B = "suspicious-transactions";
    private static final String TOPIC_C = "high-value-transactions";


    private static String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

    public static void main(String[] args) {
        IncomingTransactionsReader incomingTransactionsReader = new IncomingTransactionsReader();
        CustomerAddressDatabase customerAddressDatabase = new CustomerAddressDatabase();

        //*****************
        // YOUR CODE HERE
        //*****************

        Producer<String,Transaction> kafkaProducer = createKafkaProducer(BOOTSTRAP_SERVERS);

        try {
            processTransactions(incomingTransactionsReader, customerAddressDatabase, kafkaProducer);
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            kafkaProducer.flush();
            kafkaProducer.close();
        }
    }

    public static void processTransactions(IncomingTransactionsReader incomingTransactionsReader,
                                    CustomerAddressDatabase customerAddressDatabase,
                                    Producer<String, Transaction> kafkaProducer) throws ExecutionException, InterruptedException {

        // Retrieve the next transaction from the IncomingTransactionsReader
        // For the transaction user, get the user residence from the UserResidenceDatabase
        // Compare user residence to transaction location.
        // Send a message to the appropriate topic, depending on whether the user residence and transaction
        // location match or not.
        // Print record metadata information

        //*****************
        // YOUR CODE HERE
        //*****************

        while(incomingTransactionsReader.hasNext())
        {
            Transaction transaction = incomingTransactionsReader.next();
            String key = transaction.getUser();
            
            if(transaction.getAmount()>1000)
            {
                ProducerRecord<String, Transaction> record = new ProducerRecord(TOPIC_C, key, transaction);
                RecordMetadata recordMetadata = kafkaProducer.send(record).get();
                System.out.printf(String.format(" Record with Key %s Value: %s) was sent to (partition: %d, offset%n" +
                        "%d", record.key(), record.value(),recordMetadata.partition(), recordMetadata.offset()));
            }

            //is valid
            if(customerAddressDatabase.getUserResidence(key).equals(transaction.getTransactionLocation()))
            {
                ProducerRecord<String, Transaction> record = new ProducerRecord(TOPIC_A, key, transaction);
                RecordMetadata recordMetadata = kafkaProducer.send(record).get();
                System.out.printf(String.format(" Record with Key %s Value: %s) was sent to (partition: %d, offset%n" +
                        "%d", record.key(), record.value(),recordMetadata.partition(), recordMetadata.offset()));
            }
            //is sus
            else
            {
                ProducerRecord<String,Transaction> record = new ProducerRecord(TOPIC_B,key,transaction);

                RecordMetadata recordMetadata = kafkaProducer.send(record).get();
                System.out.printf(String.format("Record with Key %s Value: %s) was sent to (partition: %d, offset %n" +
                        "%d", record.key(), record.value(),recordMetadata.partition(), recordMetadata.offset()));
            }
        }
    }


    public static Producer<String, Transaction> createKafkaProducer(String bootstrapServers) {
    //*****************
    // YOUR CODE HERE
    //*****************
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG,"bank-producer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Transaction.TransactionSerializer.class.getName());

        return new KafkaProducer<String,Transaction>(properties);
    }

}
