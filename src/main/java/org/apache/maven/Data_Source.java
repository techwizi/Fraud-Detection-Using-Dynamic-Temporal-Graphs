package org.apache.maven;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
//import io.confluent.kafka.serializers.KafkaJsonSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class Data_Source
{

    private static final String TOPIC = "DemoData";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final Random random = new Random();

    public static void main(String[] args)
    {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ObjectMapper objectMapper = new ObjectMapper();

        String[] transactionTypes = {"purchase", "refund"};
        int[] users = generateUsers(1000000);

        try
        {
            while (true)
            {
//                ObjectNode transaction = objectMapper.createObjectNode();
//                transaction.put("src", users[random.nextInt(users.length)]);
//                transaction.put("trg", users[random.nextInt(users.length)]);
//                transaction.put("type", transactionTypes[random.nextInt(transactionTypes.length)]);
//                transaction.put("amount", Math.round(random.nextDouble() * 500.0 * 100.0) / 100.0);
//                transaction.put("timestamp", System.currentTimeMillis() / 1000);
//
//                String transactionString = transaction.toString();
                String transactionString = random.nextInt(users.length) + " " + random.nextInt(users.length) + " " + ThreadLocalRandom.current().nextLong(1000, 100000) + " " + System.currentTimeMillis();
                producer.send(new ProducerRecord<>(TOPIC, transactionString));
                System.out.println("Produced: " + transactionString);

                TimeUnit.MICROSECONDS.sleep(1);
            }
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        finally
        {
            producer.close();
        }
    }

    private static int[] generateUsers(int numUsers)
    {
        int[] users = new int[numUsers];
        for (int i = 0; i < numUsers; i++) {
            users[i] = i + 1;
        }
        return users;
    }
}

