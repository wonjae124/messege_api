package com.example.mocksend;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class MocksendApplication {

    public static void main(String[] args) {
        SpringApplication.run(MocksendApplication.class, args);
    }

}
