package com.example.kafkastream.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

@Slf4j
public class WindowTopology {

    public static String windowTopic = "window-words";

    public static Topology getTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> stream = streamsBuilder.stream(windowTopic, Consumed.with(Serdes.String(), Serdes.String()));
        tumblingWindow(stream);
        return streamsBuilder.build();

    }

    private static void tumblingWindow(KStream<String, String> stream) {
        Duration duration = Duration.ofSeconds(5);
        TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(duration);
        KTable<Windowed<String>, Long> windowedTable = stream.groupByKey().windowedBy(timeWindows).count();

        windowedTable.toStream()
                .peek((key, value) -> {
                    log.info("key: {} ,value: {}", key, value);
                    printLocalDateTimeStamp(key,value);
                })
                .print(Printed.<Windowed<String>, Long>toSysOut().withLabel("tumblingWindow"));

    }

    private static void printLocalDateTimeStamp(Windowed<String> key, Long value) {
        Instant startTime = key.window().startTime();
        Instant endTime = key.window().endTime();

        LocalDateTime startlocalDateTime = LocalDateTime.ofInstant(startTime, ZoneId.of("+04:00"));
        LocalDateTime endlocalDateTime = LocalDateTime.ofInstant(endTime, ZoneId.of("+04:00"));

        log.info("startLocalDateTime: {} ,EndLocalDateTime: {},Count: {}", startlocalDateTime, endlocalDateTime, value);

    }

}
