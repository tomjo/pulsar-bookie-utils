package net.tomjo.bookieoptimize;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.time.Clock;


@SpringBootApplication
public class BookieOptimizeApplication {

    public static void main(String[] args) {
        SpringApplication.run(BookieOptimizeApplication.class, args);
    }

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }

}
