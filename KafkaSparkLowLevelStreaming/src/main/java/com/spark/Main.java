package com.spark;

import com.spark.kafka.KafkaConfig;
import com.spark.kafka.KafkaStreamProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import java.util.Collections;
import java.util.Map;
import java.util.Collection;

public class Main {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf()
                .setAppName("KafkaDStreamsToPostgres")
                .setMaster("local[*]");
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(5));

        Map<String, Object> kafkaParams = KafkaConfig.getKafkaParams("localhost:9092", "spark-streaming-group");
        Collection<String> topics = Collections.singletonList("users");

        JavaInputDStream<ConsumerRecord<String, String>> kafkaStream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );


        KafkaStreamProcessor.processStream(kafkaStream, "jdbc:postgresql://localhost:5432/postgres", "postgres", "1902");

        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
