package com.example.batch;

import org.springframework.aop.SpringProxy;
import org.springframework.aop.framework.Advised;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersIncrementer;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.core.DecoratingProxy;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.transaction.PlatformTransactionManager;

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
    Job job(JobRepository jobRepository, CsvToDbStepConfiguration csvToDbStepConfiguration) throws Exception {
        return new JobBuilder("job", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(csvToDbStepConfiguration.csvToDb()) //
                .build();
    }

}


@Configuration
class CsvToDbStepConfiguration {

    private final DataSource dataSource;
    private final Resource resource;
    private final JobRepository repository;
    private final PlatformTransactionManager txm;

    CsvToDbStepConfiguration(@Value("file://${HOME}/Desktop/batch/data/vgsales.csv") Resource resource,
                             DataSource dataSource, JobRepository repository, PlatformTransactionManager txm) {
        this.dataSource = dataSource;
        this.repository = repository;
        this.resource = resource;
        this.txm = txm;
    }

    record CsvRow(int rank, String name, String platform, int year, String genre, String publisher,
                  float na, float eu, float jp, float other, float global) {
    }

    @Bean
    FlatFileItemReader<CsvRow> csvRowFlatFileItemReader() {
        return new FlatFileItemReaderBuilder<CsvRow>()
                .resource(resource)
                .name("csvFFIR")
                .delimited().delimiter(",")//
                .names("rank,name,platform,year,genre,publisher,na,eu,jp,other,global".split(",")) //
                .linesToSkip(1)
                .fieldSetMapper(fieldSet -> new CsvRow(
                        fieldSet.readInt("rank"),
                        fieldSet.readString("name"),
                        fieldSet.readString("platform"),
                        parseInt(fieldSet.readString("year")),
                        fieldSet.readString("genre"),
                        fieldSet.readString("publisher"),
                        fieldSet.readFloat("na"),
                        fieldSet.readFloat("eu"),
                        fieldSet.readFloat("jp"),
                        fieldSet.readFloat("other"),
                        fieldSet.readFloat("global")
                ))
                .build();

    }

    @Bean
    JdbcBatchItemWriter<CsvRow> csvRowJdbcBatchItemWriter() {
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
                 ON CONFLICT on constraint video_game_sales_name_platform_year_genre_key  do update  set 
                    rank=excluded.rank ,        
                    na_sales=excluded.na_sales ,    
                    eu_sales=excluded.eu_sales ,    
                    jp_sales=excluded.jp_sales ,    
                    other_sales=excluded.other_sales , 
                    global_sales=excluded.global_sales
                 ;                               
                                
                """;
        return new JdbcBatchItemWriterBuilder<CsvRow>()
                .sql(sql)
                .dataSource(dataSource)
                .itemSqlParameterSourceProvider(item -> {

                    var map = new HashMap<String, Object>();
                    map.putAll(Map.of(
                            "rank", item.rank(),
                            "name", item.name().trim(),
                            "platform", item.platform().trim(),
                            "year", item.year(),
                            "genre", item.genre().trim(),
                            "publisher", item.publisher().trim()
                    ));
                    map.putAll(Map.of(
                            "na_sales", item.na(),
                            "eu_sales", item.eu(),
                            "jp_sales", item.jp(),
                            "other_sales", item.other(),
                            "global_sales", item.global()

                    ));
                    return new MapSqlParameterSource(map);
                })
                .build();
    }

    @Bean
    Step csvToDb() throws Exception {
        return new StepBuilder("csvToDb", repository)
                .<CsvRow, CsvRow>chunk(100, txm)//
                .reader(csvRowFlatFileItemReader())//
                .writer(csvRowJdbcBatchItemWriter())//
                .build();
    }

    private static int parseInt(String text) {
        if (text != null && !text.contains("NA") && !text.contains("N/A"))
            return Integer.parseInt(text);
        return 0;
    }

}