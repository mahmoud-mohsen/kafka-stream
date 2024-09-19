package com.example.kafkastream.model;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record Greeting(String message, LocalDateTime timeStamp, BigDecimal transactionCount) {
}
