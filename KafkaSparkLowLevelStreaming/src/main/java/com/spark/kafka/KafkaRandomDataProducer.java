package com.spark.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.LocalTime;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import static java.time.LocalTime.now;

public class KafkaRandomDataProducer {

    public static void main(String[] args) {
        // Kafka broker URL and topic name
        String bootstrapServers = "localhost:9092";
        String topic = "users";

        // Kafka producer properties
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Create Kafka producer
        Producer<String, String> producer = new KafkaProducer<>(props);

        // Random data generation
        Random random = new Random();
        int numRecords = 100000; // Number of records to generate

        String start_time = null;
        try {
            start_time = now().toString();
            for (int i = 0; i < numRecords; i++) {
                // Generate random data
                String id = UUID.randomUUID().toString();
                String name = "User" + random.nextInt(1000);
                int age = random.nextInt(50) + 18; // Age between 18 and 67
                String createdAt = "2024-11-" + (random.nextInt(30) + 1) + "T" + (random.nextInt(24)) + ":00:00Z";
                String street = random.nextInt(999) + " Random St";
                String city = "City" + random.nextInt(100);
                String state = "State" + random.nextInt(50);
                String email = name.toLowerCase() + "@example.com";
                String phone = random.nextInt(900) + 100 + "-" + random.nextInt(900) + 100 + "-" + random.nextInt(9000) + 1000;

                // Construct JSON message
                String message = String.format(
                        "{\"id\": \"%s\", \"name\": \"%s\", \"age\": %d, \"created_at\": \"%s\", \"address\": {\"street\": \"%s\", \"city\": \"%s\", \"state\": \"%s\"}, \"contacts\": {\"email\": \"%s\", \"phone\": \"%s\"}}",
                        id, name, age, createdAt, street, city, state, email, phone
                );

                // Send to Kafka
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
                producer.send(record);

                // Log the message
                System.out.println("Produced message: " + message);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            String end_time = now().toString();
            System.out.println(start_time + end_time);
            producer.close();
        }
    }
}
