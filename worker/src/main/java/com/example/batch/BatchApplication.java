package com.example.batch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.joshlong.batch.remotechunking.worker.WorkerInboundChunkChannel;
import com.joshlong.batch.remotechunking.worker.WorkerItemProcessor;
import com.joshlong.batch.remotechunking.worker.WorkerItemWriter;
import com.joshlong.batch.remotechunking.worker.WorkerOutboundChunkChannel;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.messaging.MessageChannel;

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
    IntegrationFlow inbound(
            @WorkerInboundChunkChannel DirectChannel requests,
            ConnectionFactory connectionFactory) {
        return IntegrationFlow
                .from(Amqp.inboundAdapter(connectionFactory, "requests"))
                .channel(requests)
                .get();
    }

    @Bean
    IntegrationFlow outboundReplies(@WorkerOutboundChunkChannel MessageChannel replies,
                                    AmqpTemplate template) {
        return IntegrationFlow //
                .from(replies)
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

    private static void doSomethingTimeIntensive(YearReport yearReport) {
        System.out.println("====================");
        System.out.println("got yearReport");
        System.out.println(yearReport.toString());
    }

    @Bean
    @WorkerItemProcessor
    ItemProcessor<String, YearReport> itemProcessor() {
        return yearReportJson -> {
            System.out.println(">> processing YearReport JSON: " + yearReportJson);
            Thread.sleep(5);
            return deserializeYearReportJson(yearReportJson);
        };
    }

    @Bean
    @WorkerItemWriter
    ItemWriter<YearReport> writer() {
        return chunk -> chunk.getItems().forEach(WorkerConfiguration::doSomethingTimeIntensive);
    }
}

record YearPlatformSales(int year, String platform, float sales) {
}

record YearReport(int year, Collection<YearPlatformSales> breakout) {
}
