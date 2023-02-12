package com.example.batch;

import org.springframework.aop.SpringProxy;
import org.springframework.aop.framework.Advised;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.core.DecoratingProxy;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

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
        SpringApplication.run(BatchApplication.class, args);
    }

    @Bean
    Job job(JobRepository jobRepository, CsvToDbStepConfiguration csvToDbStepConfiguration,
            YearPlatformReportStepConfiguration yearPlatformReportStepConfiguration) throws Exception {
        return new JobBuilder("job", jobRepository)//
                .incrementer(new RunIdIncrementer())//
                .start(csvToDbStepConfiguration.gameByYearStep()) //
                .next(yearPlatformReportStepConfiguration.yearPlatformReportStep()) //
                .build();
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

@Configuration
class CsvToDbStepConfiguration {

    private final DataSource dataSource;
    private final Resource resource;
    private final JobRepository repository;
    private final PlatformTransactionManager tx;

    CsvToDbStepConfiguration(
            @Value("file:///${HOME}/Desktop/batch/data/vgsales.csv") Resource resource,
            DataSource dataSource, JobRepository repository,
            PlatformTransactionManager txm) throws Exception {
        this.dataSource = dataSource;
        this.repository = repository;
        this.resource = resource;
        this.tx = txm;
    }

    record GameByYear(int rank, String name, String platform, int year, String genre, String publisher, float na,
                      float eu, float jp, float other, float global) {
    }

    @Bean
    @StepScope
    FlatFileItemReader<GameByYear> gameByYearReader() {
        return new FlatFileItemReaderBuilder<GameByYear>()//
                .resource(resource)//
                .name("csvFFIR")//
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
                }).build();
    }

    @Bean
    Step gameByYearStep() {
        return new StepBuilder("csvToDb", repository)//
                .<GameByYear, GameByYear>chunk(100, tx)//
                .reader(gameByYearReader())//
                .writer(gameByYearWriter())//
                .build();
    }


    private static int parseInt(String text) {
        if (text != null && !text.contains("NA") && !text.contains("N/A")) return Integer.parseInt(text);
        return 0;
    }

}