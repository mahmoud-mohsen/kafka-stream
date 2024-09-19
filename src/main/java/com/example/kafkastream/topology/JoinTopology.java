package com.example.kafkastream.topology;

import com.example.kafkastream.model.Alphabet;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;

public class JoinTopology {

    public static String alphabets = "alphabets";
    public static String alphabets_abbreviation = "alphabets_abbreviation";

    public static Topology getTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

//        joinKStreamWithKTable(streamsBuilder);
//        joinKStreamWithGlobalTable(streamsBuilder);
//        joinKTableWithKTable(streamsBuilder);
        joinKStreamWithKStream(streamsBuilder);
        return streamsBuilder.build();

    }

    private static void joinKStreamWithKTable(StreamsBuilder streamsBuilder) {
        KStream<String, String> alphabetsAbbreviationKStream = streamsBuilder.stream(alphabets_abbreviation, Consumed.with(Serdes.String(), Serdes.String()));
        alphabetsAbbreviationKStream.print(Printed.<String, String>toSysOut().withLabel("alphabets_abbreviation"));

        KTable<String, String> alphabetsTable = streamsBuilder.table(alphabets, Consumed.with(Serdes.String(), Serdes.String()), Materialized.as("alphabets"));
        alphabetsTable.toStream().print(Printed.<String, String>toSysOut().withLabel("alphabets"));

        KStream<String, Alphabet> join = alphabetsAbbreviationKStream.join(alphabetsTable, (s, s2) -> new Alphabet(s, s2));
        join.print(Printed.<String, Alphabet>toSysOut().withLabel("join"));

    }

    private static void joinKStreamWithGlobalTable(StreamsBuilder streamsBuilder) {
        KStream<String, String> alphabetsAbbreviationKStream = streamsBuilder.stream(alphabets_abbreviation, Consumed.with(Serdes.String(), Serdes.String()));
        alphabetsAbbreviationKStream.print(Printed.<String, String>toSysOut().withLabel("alphabets_abbreviation"));

        GlobalKTable<String, String> alphabetsTable = streamsBuilder.globalTable(alphabets, Consumed.with(Serdes.String(), Serdes.String()), Materialized.as("alphabets"));

        KStream<String, Alphabet> join = alphabetsAbbreviationKStream
                .leftJoin(alphabetsTable
                        , (s, s2) -> s
                        , (s, s2) -> new Alphabet(s, s2));
        join.print(Printed.<String, Alphabet>toSysOut().withLabel("join"));

    }

    private static void joinKTableWithKTable(StreamsBuilder streamsBuilder) {
        KTable<String, String> alphabetsAbbreviationKTable = streamsBuilder.table(alphabets_abbreviation, Consumed.with(Serdes.String(), Serdes.String()), Materialized.as("alphabets_abbreviation-store"));

        KTable<String, String> alphabetsTable = streamsBuilder.table(alphabets, Consumed.with(Serdes.String(), Serdes.String()), Materialized.as("alphabets"));
        alphabetsTable.toStream().print(Printed.<String, String>toSysOut().withLabel("alphabets"));

        KTable<String, Alphabet> join = alphabetsAbbreviationKTable.join(alphabetsTable, (s, s2) -> new Alphabet(s, s2));
        join.toStream().print(Printed.<String, Alphabet>toSysOut().withLabel("join"));

    }

    private static void joinKStreamWithKStream(StreamsBuilder streamsBuilder) {
        KStream<String, String> alphabetsAbbreviationKStream = streamsBuilder.stream(alphabets_abbreviation, Consumed.with(Serdes.String(), Serdes.String()));
        alphabetsAbbreviationKStream.print(Printed.<String, String>toSysOut().withLabel("alphabets_abbreviation"));

        KStream<String, String> alphabetsStream = streamsBuilder.stream(alphabets, Consumed.with(Serdes.String(), Serdes.String()));
        alphabetsStream.print(Printed.<String, String>toSysOut().withLabel("alphabets"));

        ValueJoiner<String, String, Alphabet> valueJoiner = Alphabet::new;
        JoinWindows joinWindows = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5));
        StreamJoined<String, String, String> joinedParam = StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
                .withName("alphabets-join")
                .withStoreName("alphabets-join");

        KStream<String, Alphabet> join = alphabetsAbbreviationKStream.outerJoin(alphabetsStream,
                valueJoiner, joinWindows, joinedParam);

        join.print(Printed.<String, Alphabet>toSysOut().withLabel("join"));

    }


}
