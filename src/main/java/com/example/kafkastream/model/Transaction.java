package com.example.kafkastream.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;

@AllArgsConstructor
@Getter
@Setter
public class Transaction {
    String status;
    BigDecimal count;

    public Transaction() {
        this.status = "";
        this.count = BigDecimal.ZERO;
    }
}
