package com.example.batch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.joshlong.batch.remotechunking.worker.WorkerInboundChunkChannel;
import com.joshlong.batch.remotechunking.worker.WorkerItemProcessor;
import com.joshlong.batch.remotechunking.worker.WorkerItemWriter;
import com.joshlong.batch.remotechunking.worker.WorkerOutboundChunkChannel;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.aop.SpringProxy;
import org.springframework.aop.framework.Advised;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.core.DecoratingProxy;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.messaging.MessageChannel;

import java.util.Collection;
import java.util.Set;

@SpringBootApplication
@ImportRuntimeHints(BatchApplication.Hints.class)
public class BatchApplication {


    static class Hints implements RuntimeHintsRegistrar {

        @Override
        public void registerHints(RuntimeHints hints, ClassLoader classLoader) {
            // todo https://github.com/spring-projects/spring-batch/issues/4294
            hints.proxies().registerJdkProxy(JobOperator.class, SpringProxy.class, Advised.class, DecoratingProxy.class);
            Set.of(YearReport.class,  YearPlatformSales.class)
                    .forEach(clzz -> hints.reflection().registerType(clzz, MemberCategory.values()));

        }
    }

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
