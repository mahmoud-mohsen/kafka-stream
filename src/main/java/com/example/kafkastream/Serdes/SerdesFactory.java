package com.example.kafkastream.Serdes;

import com.example.kafkastream.model.Greeting;
import com.example.kafkastream.model.Transaction;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class SerdesFactory<T> {

    public static Serde<Greeting> getGreetingJsonSerdes() {
        JsonSerializer<Greeting> jsonSerializer = new JsonSerializer();
        JsonDeserializer<Greeting> jsonDeserializer = new JsonDeserializer<>(Greeting.class);
        return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
    }

    public static Serde<Transaction> getTransactionJsonSerdes() {
        JsonSerializer<Transaction> jsonSerializer = new JsonSerializer();
        JsonDeserializer<Transaction> jsonDeserializer = new JsonDeserializer<>(Transaction.class);
        return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
    }
}
