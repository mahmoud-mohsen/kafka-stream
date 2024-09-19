package com.example.kafkastream.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class Alphabet {

    private String abbreviation;
    private String description;

    @Override
    public String toString() {
        return "Alphabet{" +
                "abbreviation='" + abbreviation + '\'' +
                ", description='" + description + '\'' +
                '}';
    }


}
