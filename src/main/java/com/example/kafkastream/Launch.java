package com.example.kafkastream;

import com.example.kafkastream.Serdes.SerdesFactory;
import com.example.kafkastream.topology.GreetingTopology;
import com.example.kafkastream.topology.JoinTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;

import java.util.List;
import java.util.Properties;

@Slf4j
public class Launch {
    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "greeting-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
//        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
//        properties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SerdesFactory.getGreetingJsonSerdes().getClass());
//        createTopic(properties, List.of(GreetingTopology.sourceTopic, GreetingTopology.destinationTopic,GreetingTopology.words));


//        KafkaStreams kafkaStreams = new KafkaStreams(GreetingTopology.getGreetingTopology(), properties);
        KafkaStreams kafkaStreams = new KafkaStreams(JoinTopology.getTopology(), properties);

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        try {
            kafkaStreams.start();
        } catch (Exception ex) {
            log.error("Exception happened while starting kafka stream: {}", ex.getMessage(), ex);
        }

    }


    public static void createTopic(Properties properties, List<String> topics) {
        AdminClient adminClient = AdminClient.create(properties);
        int partitions = 2;
        short replicas = 1;

        List<NewTopic> listTopics = topics.stream().map(topic -> new NewTopic(topic, partitions, replicas)).toList();

        CreateTopicsResult createTopicsResult = adminClient.createTopics(listTopics);
        try {
            createTopicsResult.all().get();
            log.info("Topics created successfully");
        } catch (Exception ex) {
            log.error("Error creating topics", ex);
        }

    }

}
