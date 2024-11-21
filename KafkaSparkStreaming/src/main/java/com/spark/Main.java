package com.spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;
import java.util.concurrent.TimeoutException;

public class Main {
    public static void main(String[] args) throws TimeoutException {
        // Initialize SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("KafkaNestedJsonToPostgres")
                .master("local[*]") // Use a cluster master URL for production
                .getOrCreate();

        // Read data from Kafka
        Dataset<Row> kafkaStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092") // Replace with your Kafka broker
                .option("subscribe", "users") // Replace with your Kafka topic name
                .option("startingOffsets", "earliest")
                .load();

        // Extract JSON data from Kafka message
        Dataset<Row> jsonStream = kafkaStream.selectExpr("CAST(value AS STRING) as jsonData");

        // Define a schema for the nested JSON data
        StructType nestedJsonSchema = new StructType()
                .add("id", "string")
                .add("name", "string")
                .add("age", "integer")
                .add("created_at", "string")
                .add("address", new StructType()
                        .add("street", "string")
                        .add("city", "string")
                        .add("state", "string"))
                .add("contacts", new StructType()
                        .add("email", "string")
                        .add("phone", "string"));

        // Parse JSON and apply schema
        Dataset<Row> parsedStream = jsonStream.select(functions.from_json(
                        functions.col("jsonData"), nestedJsonSchema).as("data"))
                .select("data.*");

        // Flatten the nested structure
        Dataset<Row> flattenedStream = parsedStream
                .withColumn("created_at", functions.to_timestamp(functions.col("created_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
                .withColumn("street", functions.col("address.street"))
                .withColumn("city", functions.col("address.city"))
                .withColumn("state", functions.col("address.state"))
                .withColumn("email", functions.col("contacts.email"))
                .withColumn("phone", functions.col("contacts.phone"))
                .drop("address", "contacts"); // Remove nested fields after flattening

//        kafkaStream.printSchema();
//        parsedStream.printSchema();
        // Write flattened data to PostgreSQL in real-time
        StreamingQuery query = flattenedStream.writeStream()
                .foreachBatch((batchDF, batchId) -> {
                    batchDF.write()
                            .format("jdbc")
                            .option("driver", "org.postgresql.Driver")
                            .option("url", "jdbc:postgresql://localhost:5432/postgres") // Replace with your database URL
                            .option("dbtable", "customer.flattened_user_data")
                            .option("user", "postgres") // Replace with your database username
                            .option("password", "1902") // Replace with your database password
                            .mode(SaveMode.Append)
                            .save();
                })
                .trigger(Trigger.ProcessingTime("5 seconds"))
                .start();

        // Await termination
        try {
            query.awaitTermination();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
