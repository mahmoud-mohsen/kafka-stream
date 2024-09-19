package com.example.kafkastream.Serdes;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

@Slf4j
public class JsonDeserializer<T> implements Deserializer<T> {
    ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule())
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    private Class<T> classType;

    public JsonDeserializer(Class<T> classType) {
        this.classType = classType;
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        try {
            return objectMapper.readValue(bytes, classType);
        } catch (StreamReadException e) {
            log.error("JsonDeserializer StreamReadException :: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        } catch (DatabindException e) {
            log.error("JsonDeserializer DatabindException :: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        } catch (IOException e) {
            log.error("JsonDeserializer IOException :: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        } catch (Exception e) {
            log.error("JsonDeserializer Exception :: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
