package com.example.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.aop.SpringProxy;
import org.springframework.aop.framework.Advised;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.integration.chunk.ChunkMessageChannelItemWriter;
import org.springframework.batch.integration.chunk.RemoteChunkHandlerFactoryBean;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.batch.JobExecutionEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.context.event.EventListener;
import org.springframework.core.DecoratingProxy;
import org.springframework.core.io.Resource;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.core.MessagingTemplate;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@SpringBootApplication
@ImportRuntimeHints(BatchApplication.Hints.class)
public class BatchApplication {

    static class Hints implements RuntimeHintsRegistrar {

        @Override
        public void registerHints(RuntimeHints hints, ClassLoader classLoader) {
            // todo https://github.com/spring-projects/spring-batch/issues/4294
            hints.proxies().registerJdkProxy(JobOperator.class, SpringProxy.class, Advised.class, DecoratingProxy.class);
        }
    }

    public static void main(String[] args) {
        System.setProperty("csvFile", "file:///Users/jlong/Desktop/lets-code-spring-batch/data/vgsales.csv");
        SpringApplication.run(BatchApplication.class, args);
    }

    public static final String EMPTY_CSV_STATUS = "EMPTY";

    @Bean
    Job job(
            JobRepository jobRepository,
            ErrorStepConfiguration errorStepConfiguration,
            CsvToDbStepConfiguration csvToDbStepConfiguration,
            YearPlatformReportStepConfiguration yearPlatformReportStepConfiguration,
            YearReportStepConfiguration yearReportStepConfiguration,
            EndStepConfiguration endStepConfiguration) {
        var gameByYearStep = csvToDbStepConfiguration.gameByYearStep();
        return new JobBuilder("job", jobRepository)//
                .incrementer(new RunIdIncrementer())//
                .start(gameByYearStep).on(EMPTY_CSV_STATUS).to(errorStepConfiguration.errorStep())  //
                .from(gameByYearStep).on("*").to(yearPlatformReportStepConfiguration.yearPlatformReportStep()) //
                .next(yearReportStepConfiguration.yearReportStep())
                .next(endStepConfiguration.end()) //
                .build() //
                .build();

    }
}

record YearPlatformSales(int year, String platform, float sales) {
}

record YearReport(int year, Collection<YearPlatformSales> breakout) {
}

@Configuration
class YearReportStepConfiguration {

    // todo: clear this out after the job is done. some sort of listener?
    private final Map<Integer, YearReport> reportMap = new ConcurrentHashMap<>();
    private final DataSource dataSource;
    private final JobRepository repository;
    private final PlatformTransactionManager transactionManager;
    private final ObjectMapper objectMapper;

    YearReportStepConfiguration(JobRepository repository, DataSource dataSource, PlatformTransactionManager transactionManager, ObjectMapper objectMapper) {
        this.dataSource = dataSource;
        this.repository = repository;
        this.transactionManager = transactionManager;
        this.objectMapper = objectMapper;
    }

    @EventListener
    void batchJobCompleted(JobExecutionEvent event) {
        var running = Map.of(//
                "running", event.getJobExecution().getStatus().isRunning(),//
                "finished", event.getJobExecution().getExitStatus().getExitCode() //
        );//
        System.out.println("jobExecutionEvent: [" + running + "]");
        this.reportMap.clear();
    }

    private final RowMapper<YearReport> rowMapper = (rs, rowNum) -> {
        var year = rs.getInt("year");
        if (!this.reportMap.containsKey(year))
            this.reportMap.put(year, new YearReport(year, new ArrayList<>()));
        var yr = this.reportMap.get(year);
        yr.breakout().add(new YearPlatformSales(rs.getInt("year"), rs.getString("platform"), rs.getFloat("sales")));
        return yr;
    };

    @Bean
    ItemReader<YearReport> yearPlatformSalesItemReader() {
        var sql = """
                select year   ,
                       ypr.platform,
                       ypr.sales,
                       (select count(yps.year) from year_platform_report yps where yps.year = ypr.year ) 
                from year_platform_report ypr
                where ypr.year != 0
                order by year
                """;
        return new JdbcCursorItemReaderBuilder<YearReport>()
                .sql(sql)
                .name("yearPlatformSalesItemReader")
                .dataSource(this.dataSource)
                .rowMapper(this.rowMapper)
                .build();

    }

    @Bean
    TaskletStep yearReportStep() {
        return new StepBuilder("yearReportStep", repository)
                .<YearReport, String>chunk(1000, this.transactionManager)
                .reader(yearPlatformSalesItemReader())
                .processor(objectMapper::writeValueAsString)
                .writer(chunkMessageChannelItemWriter() )
                .build();
    }

    @Bean
    IntegrationFlow replyFlow(ConnectionFactory connectionFactory) {
        return IntegrationFlow
                .from(Amqp.inboundAdapter(connectionFactory, "replies"))
                .channel(replies())
                .get();
    }

    @Bean
    DirectChannel requests() {
        return MessageChannels.direct().get();
    }

    @Bean
    QueueChannel replies() {
        return MessageChannels.queue().get();
    }

    static class DedupingChunkMessageChannelItemWriter<T> extends ChunkMessageChannelItemWriter<T> {

        @Override
        public void write(Chunk<? extends T> items) throws Exception {
            var inputCollection = items.getItems();
            var newList = new ArrayList<T>(new LinkedHashSet<>(inputCollection));
            super.write(new Chunk<T>(newList));
        }
    }


    @Bean
    @StepScope
    ChunkMessageChannelItemWriter<String> chunkMessageChannelItemWriter() {
        var chunkMessageChannelItemWriter = new DedupingChunkMessageChannelItemWriter<String>();
        chunkMessageChannelItemWriter.setMessagingOperations(messagingTemplate());
        chunkMessageChannelItemWriter.setReplyChannel(replies());
        return chunkMessageChannelItemWriter;
    }

    @Bean
    RemoteChunkHandlerFactoryBean<String> chunkHandler() throws Exception {
        var chunkMessageChannelItemWriterProxy = (chunkMessageChannelItemWriter());
        var remoteChunkHandlerFactoryBean = new RemoteChunkHandlerFactoryBean<String>();
        remoteChunkHandlerFactoryBean.setChunkWriter(chunkMessageChannelItemWriterProxy);
        remoteChunkHandlerFactoryBean.setStep(this.yearReportStep());
        return remoteChunkHandlerFactoryBean;
    }

    @Bean
    MessagingTemplate messagingTemplate() {
        var template = new MessagingTemplate();
        template.setDefaultChannel(requests());
        template.setReceiveTimeout(2000);
        return template;
    }

    @Bean
    IntegrationFlow outboundFlow(AmqpTemplate amqpTemplate) {
        return IntegrationFlow //
                .from(requests())
                .handle(Amqp.outboundAdapter(amqpTemplate).routingKey("requests"))
                .get();
    }
}

/**
 * installs all the infrastructure for RabbitMQ
 */
@Configuration
class RabbitConfiguration {

    @Bean
    org.springframework.amqp.core.Queue requestQueue() {
        return new org.springframework.amqp.core.Queue("requests", false);
    }

    @Bean
    org.springframework.amqp.core.Queue repliesQueue() {
        return new Queue("replies", false);
    }

    @Bean
    TopicExchange exchange() {
        return new TopicExchange("remote-chunking-exchange");
    }

    @Bean
    Binding repliesBinding(TopicExchange exchange) {
        return BindingBuilder.bind(repliesQueue()).to(exchange).with("replies");
    }

    @Bean
    Binding requestBinding(TopicExchange exchange) {
        return BindingBuilder.bind(requestQueue()).to(exchange).with("requests");
    }


}

record GameByYear(int rank, String name, String platform, int year, String genre, String publisher, float na,
                  float eu, float jp, float other, float global) {
}


@Configuration
class EndStepConfiguration {

    private final JobRepository repository;
    private final PlatformTransactionManager tx;

    EndStepConfiguration(JobRepository repository, PlatformTransactionManager tx) {
        this.repository = repository;
        this.tx = tx;
    }

    @Bean
    Step end() {
        return new StepBuilder("end", repository)
                .tasklet((contribution, chunkContext) -> {
                    System.out.println("the job is finished");
                    return RepeatStatus.FINISHED;
                }, tx)
                .build();
    }


}

@Configuration
class ErrorStepConfiguration {

    private final JobRepository repository;
    private final PlatformTransactionManager tx;

    ErrorStepConfiguration(JobRepository repository, PlatformTransactionManager tx) {
        this.repository = repository;
        this.tx = tx;
    }

    @Bean
    Step errorStep() {
        return new StepBuilder("errorStep", repository)
                .tasklet((contribution, chunkContext) -> {
                    System.out.println("oops!");
                    return RepeatStatus.FINISHED;
                }, tx)
                .build();
    }
}

@Configuration
class CsvToDbStepConfiguration {

    private final DataSource dataSource;
    private final Resource resource;
    private final JobRepository repository;
    private final PlatformTransactionManager tx;
    private final JdbcTemplate jdbc;

    CsvToDbStepConfiguration(
            @Value("${csvFile}") Resource resource,
            DataSource dataSource, JobRepository repository,
            PlatformTransactionManager txm, JdbcTemplate template) {
        this.dataSource = dataSource;
        this.repository = repository;
        this.resource = resource;
        this.tx = txm;
        this.jdbc = template;
    }

    @Bean
    @StepScope
    FlatFileItemReader<GameByYear> gameByYearReader() {
        return new FlatFileItemReaderBuilder<GameByYear>()//
                .resource(resource)//
                .name("gameByYearReader")//
                .delimited().delimiter(",")//
                .names("rank,name,platform,year,genre,publisher,na,eu,jp,other,global".split(",")) //
                .linesToSkip(1)//
                .fieldSetMapper(fieldSet -> new GameByYear(//
                        fieldSet.readInt("rank"),//
                        fieldSet.readString("name"),//
                        fieldSet.readString("platform"),
                        parseInt(fieldSet.readString("year")),//
                        fieldSet.readString("genre"),//
                        fieldSet.readString("publisher"),//
                        fieldSet.readFloat("na"),//
                        fieldSet.readFloat("eu"),//
                        fieldSet.readFloat("jp"),//
                        fieldSet.readFloat("other"),//
                        fieldSet.readFloat("global")
                ))//
                .build();

    }

    @Bean
    JdbcBatchItemWriter<GameByYear> gameByYearWriter() {
        var sql = """
                insert into video_game_sales(
                    rank          ,
                    name          ,
                    platform      ,
                    year          ,
                    genre         ,
                    publisher     ,
                    na_sales      ,
                    eu_sales      ,
                    jp_sales      ,
                    other_sales   ,
                    global_sales
                )
                 values (
                    :rank,        
                    :name,        
                    :platform,    
                    :year,        
                    :genre,       
                    :publisher,   
                    :na_sales,    
                    :eu_sales,    
                    :jp_sales,    
                    :other_sales, 
                    :global_sales
                 ) 
                 ON CONFLICT ON CONSTRAINT video_game_sales_name_platform_year_genre_key  
                 DO UPDATE  
                 SET 
                    rank=excluded.rank ,        
                    na_sales=excluded.na_sales ,    
                    eu_sales=excluded.eu_sales ,    
                    jp_sales=excluded.jp_sales ,    
                    other_sales=excluded.other_sales , 
                    global_sales=excluded.global_sales
                 ;                               
                """;
        return new JdbcBatchItemWriterBuilder<GameByYear>()//
                .sql(sql)//
                .dataSource(dataSource)//
                .itemSqlParameterSourceProvider(item -> {
                    var map = new HashMap<String, Object>();
                    map.putAll(Map.of(
                            "rank", item.rank(),//
                            "name", item.name().trim(),//
                            "platform", item.platform().trim(),//
                            "year", item.year(),//
                            "genre", item.genre().trim(),//
                            "publisher", item.publisher().trim()//
                    ));
                    map.putAll(Map.of(
                            "na_sales", item.na(),//
                            "eu_sales", item.eu(),//
                            "jp_sales", item.jp(),//
                            "other_sales", item.other(),//
                            "global_sales", item.global()//
                    ));
                    return new MapSqlParameterSource(map);
                }) //
                .build();
    }

    @Bean
    Step gameByYearStep() {
        return new StepBuilder("csvToDb", repository)//
                .<GameByYear, GameByYear>chunk(100, tx)//
                .reader(gameByYearReader())//
                .writer(gameByYearWriter())//
                .listener(new StepExecutionListener() {
                    @Override
                    public ExitStatus afterStep(StepExecution stepExecution) {
                        var count = Objects.requireNonNull(
                                jdbc.queryForObject("select coalesce(count(*) ,0) from video_game_sales", Integer.class));
                        var status = count == 0 ? new ExitStatus(BatchApplication.EMPTY_CSV_STATUS) : ExitStatus.COMPLETED;
                        System.out.println("the status is " + status);
                        return status;
                    }
                })
                .build();
    }

    private static int parseInt(String text) {
        if (text != null && !text.contains("NA") && !text.contains("N/A")) return Integer.parseInt(text);
        return 0;
    }

}

@Configuration
class YearPlatformReportStepConfiguration {

    private final JobRepository repository;
    private final JdbcTemplate jdbc;
    private final PlatformTransactionManager transactionManager;
    private final TransactionTemplate tx;

    YearPlatformReportStepConfiguration(JobRepository repository, JdbcTemplate jdbc, PlatformTransactionManager transactionManager, TransactionTemplate tx) {
        this.repository = repository;
        this.jdbc = jdbc;
        this.transactionManager = transactionManager;
        this.tx = tx;
    }

    @Bean
    Step yearPlatformReportStep() {
        return new StepBuilder("yearPlatformReportStep", repository)//
                .tasklet((contribution, chunkContext) ->//
                        tx.execute(status -> {
                            jdbc.execute(
                                    """
                                                insert into year_platform_report (year, platform)
                                                select year, platform from video_game_sales
                                                on conflict on constraint year_platform_report_year_platform_key do nothing;
                                            """);
                            jdbc.execute("""
                                    insert into year_platform_report (year, platform, sales)
                                    select yp1.year,
                                           yp1.platform, (
                                                select sum(vgs.global_sales) from video_game_sales vgs
                                                where vgs.platform = yp1.platform and vgs.year = yp1.year
                                            )
                                    from year_platform_report as yp1
                                    on conflict on constraint year_platform_report_year_platform_key
                                     do update set 
                                                year = excluded.year,
                                            platform = excluded.platform,
                                               sales = excluded.sales;
                                    """);
                            return RepeatStatus.FINISHED;
                        }), transactionManager)//
                .build();
    }
}
