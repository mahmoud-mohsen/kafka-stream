package com.example.kafkastream.topology;

import com.example.kafkastream.Serdes.SerdesFactory;
import com.example.kafkastream.model.Greeting;
import com.example.kafkastream.model.Transaction;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

public class GreetingTopology {

    public static String sourceTopic = "greeting";
    public static String destinationTopic = "greeting_uppercase";
    public static String activeTopic = "active";
    public static String inactiveTopic = "inactive";
    public static String words = "words";


    public static Topology getGreetingTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, Greeting> sourceStream = streamsBuilder.stream(sourceTopic
                , Consumed.with(Serdes.String(), SerdesFactory.getGreetingJsonSerdes())
        );
        sourceStream.print(Printed.<String, Greeting>toSysOut().withLabel("sourceStream"));

        sourceStream.split()
                .branch((s, greeting) ->
                                greeting.message().equalsIgnoreCase("ACTIVE")
                        , Branched.withConsumer(consumer -> {

                                    aggregate(consumer, "active-aggregate");

                                    consumer.to(activeTopic, Produced.with(Serdes.String(), SerdesFactory.getGreetingJsonSerdes()));
                                }
                        ))
                .branch((s, greeting) ->
                                greeting.message().equalsIgnoreCase("INACTIVE")
                        , Branched.withConsumer(consumer -> {
                                    KGroupedStream<String, Greeting> kTable = consumer
                                            .map((s, greeting) -> new KeyValue<>(greeting.message(), greeting))
                                            .groupByKey();

                                    aggregate(consumer, "inActive-aggregate");
                                                                    consumer.to(inactiveTopic, Produced.with(Serdes.String(), SerdesFactory.getGreetingJsonSerdes()));

                                }
                        ));

//        KGroupedStream<String, Greeting> groupedStream =
//                sourceStream.groupByKey(Grouped.with(Serdes.String(), SerdesFactory.getGreetingJsonSerdes()));

//        count(groupedStream);
//        reduce(groupedStream);

        return streamsBuilder.build();

    }

    private static void aggregate(KStream<String, Greeting> kTable, String type) {

        Initializer<Transaction> transactionInitializer = Transaction::new;

        Aggregator<String, Greeting, Transaction> stringGreetingTransactionAggregator = (s, greeting, transaction) -> new Transaction(greeting.message(), transaction.getCount().add(greeting.transactionCount()));

        KTable<String, Transaction> aggregate = kTable
                .map((s, greeting) -> KeyValue.pair(greeting.message(), greeting))
                .groupByKey(Grouped.with(Serdes.String(),SerdesFactory.getGreetingJsonSerdes()))
                .aggregate(transactionInitializer, stringGreetingTransactionAggregator
                , Materialized.<String, Transaction, KeyValueStore<Bytes, byte[]>>as(type)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(SerdesFactory.getTransactionJsonSerdes()));

        aggregate.toStream().print(Printed.<String, Transaction>toSysOut().withLabel(type));
    }

    private static void reduce(KGroupedStream<String, Greeting> groupedStream) {
        KTable<String, Greeting> kTable = groupedStream.reduce((greeting, greeting2) -> new Greeting(greeting2.message(), greeting2.timeStamp(), greeting.transactionCount().add(greeting2.transactionCount())));
        kTable.toStream().print(Printed.<String, Greeting>toSysOut().withLabel("kTable-reduce"));
    }

    private static void count(KGroupedStream<String, Greeting> groupedStream) {
        KTable<String, Long> kTable = groupedStream.count(Named.as("greeting-count"));
        kTable.toStream().print(Printed.<String, Long>toSysOut().withLabel("kTable-count"));
    }

    public static Topology getKTableTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KTable<String, String> kTable = streamsBuilder.table(words
                , Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.as("words-store")
        );

        kTable.toStream().print(Printed.<String, String>toSysOut().withLabel("kTable"));
        return streamsBuilder.build();

    }

}
