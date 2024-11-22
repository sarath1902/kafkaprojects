package com.spark.kafka;

import com.spark.db.PostgresWriter;
import com.spark.utils.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.api.java.JavaRDD;

import java.util.Iterator;

public class KafkaStreamProcessor {

    public static void processStream(JavaInputDStream<ConsumerRecord<String, String>> kafkaStream, String jdbcUrl, String user, String password) {
        kafkaStream.foreachRDD((JavaRDD<ConsumerRecord<String, String>> rdd) -> {
            rdd.foreachPartition(partition -> {
                // Initialize database connection per partition
                PostgresWriter postgresWriter = new PostgresWriter(jdbcUrl, user, password);
                try {
                    processPartition(partition, postgresWriter);
                } finally {
                    // Ensure the writer is closed
                    postgresWriter.close();
                }
            });
        });
    }

    private static void processPartition(Iterator<ConsumerRecord<String, String>> partition, PostgresWriter postgresWriter) {
        while (partition.hasNext()) {
            ConsumerRecord<String, String> record = partition.next();
            try {
                // Parse the incoming JSON data
                JsonParser.ParsedRecord parsedRecord = JsonParser.parseJson(record.value());

                // If valid data, write to database
                if (parsedRecord != null) {
                    postgresWriter.writeData(
                            parsedRecord.id, parsedRecord.name, parsedRecord.age, parsedRecord.createdAt,
                            parsedRecord.street, parsedRecord.city, parsedRecord.state, parsedRecord.email, parsedRecord.phone
                    );
                }
            } catch (Exception e) {
                // When the JSON is invalid, write raw data to an audit table
                writeToAuditTable(record.value(), postgresWriter);
            }
        }
    }

    private static void writeToAuditTable(String rawJson, PostgresWriter postgresWriter) {
        try {
            postgresWriter.writeRawJsonData(rawJson); // Method to write raw data to an audit table
        } catch (Exception e) {
            System.err.println("Failed to write raw data to audit table: " + e.getMessage());
        }
    }
}
