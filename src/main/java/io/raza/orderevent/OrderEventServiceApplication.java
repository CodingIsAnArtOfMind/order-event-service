package io.raza.orderevent;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class OrderEventServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderEventServiceApplication.class, args);
    }

}
