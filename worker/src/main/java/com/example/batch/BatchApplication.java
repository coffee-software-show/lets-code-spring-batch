package com.example.batch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.batch.core.step.item.SimpleChunkProcessor;
import org.springframework.batch.integration.chunk.ChunkProcessorChunkHandler;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;

import java.util.Collection;

@SpringBootApplication
public class BatchApplication {

    public static void main(String[] args) {
        SpringApplication.run(BatchApplication.class, args);
    }
}


@Configuration
class WorkerConfiguration {

    private final ObjectMapper objectMapper;

    WorkerConfiguration(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Bean
    DirectChannel requests() {
        return MessageChannels.direct().get();
    }

    @Bean
    DirectChannel replies() {
        return MessageChannels.direct().get();
    }

    @Bean
    IntegrationFlow messagesIn(ConnectionFactory connectionFactory) {
        return IntegrationFlow
                .from(Amqp.inboundAdapter(connectionFactory, "requests"))
                .channel(requests())
                .get();
    }

    @Bean
    IntegrationFlow outgoingReplies(AmqpTemplate template) {
        return IntegrationFlow //
                .from(replies())
                .handle(Amqp.outboundAdapter(template).routingKey("replies"))
                .get();
    }

    private YearReport deserializeYearReportJson(String json) {
        try {
            return objectMapper.readValue(json, YearReport.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("oops! couldn't parse the JSON!", e);
        }
    }

    @Bean
    @ServiceActivator(inputChannel = "requests", outputChannel = "replies", sendTimeout = "10000")
    ChunkProcessorChunkHandler<String> chunkProcessorChunkHandler() {

        var itemProcessor = (ItemProcessor<String, YearReport>) (yearReportJson) -> {
            System.out.println(">> processing YearReport JSON: " + yearReportJson);
            Thread.sleep(5);
            return deserializeYearReportJson(yearReportJson);
        };

        var itemWriter = this.writer();

        var chunkProcessorChunkHandler = new ChunkProcessorChunkHandler<String>();
        chunkProcessorChunkHandler.setChunkProcessor(new SimpleChunkProcessor<>(itemProcessor, itemWriter));
        return chunkProcessorChunkHandler;
    }

    @Bean
    ItemWriter<YearReport> writer() {
        return chunk -> chunk.getItems().forEach(System.out::println);
    }
}

record YearPlatformSales(int year, String platform, float sales) {
}

record YearReport(int year, Collection<YearPlatformSales> breakout) {
}

record GameByYear(int rank, String name, String platform, int year, String genre, String publisher, float na,
                  float eu, float jp, float other, float global) {
}

