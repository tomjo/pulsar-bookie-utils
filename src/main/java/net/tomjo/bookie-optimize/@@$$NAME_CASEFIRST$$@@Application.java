package net.tomjo.Bookie-optimize;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.time.Clock;

import static io.argoproj.workflow.JSON.createGson;

@SpringBootApplication
public class Bookie-optimizeApplication {

    public static void main(String[] args) {
        SpringApplication.run(Bookie-optimizeApplication.class, args);
    }

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }

}
