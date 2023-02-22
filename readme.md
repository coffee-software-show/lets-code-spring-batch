# Spring Batch, up and running 


## Notes 

I took this data that we use in this application from [https://www.kaggle.com/datasets/gregorut/videogamesales](this dataset). I have no idea if it works or not. 

## Outline

## part 1
* motivating spring batch
* a new project on the spring initializr
* `docker-compose.yml`
* setup the db connection properties
* tell batch to initialize its own schema
* introducing the spring batch metadata tables
* building a `Job` with the `JobBuilder` and `StepBuilder`
 ** a "hello, world!" `Tasklet`
* see the results in the `job_execution\*` tables in the SQL DB.
* giving our batch job a unique run id
* `JobParameters`


## part 2
* `StepScope` and SpEL (`#{ jobParameters['date'] }`).
* running the job yourself with the `JobLauncher` and disabling the property in `application.properties`.
* introduce `.chunk<T>(N)` 
* introduce `ItemReader<T>` and `ItemProcessor<T>` and `ItemWriter<T>`
* using the `ListItemReader<T>` to read some hardcoded data ready manually from our `.csv`
* writing my own `ItemWriter<T>` to dump the data that was read in
* creating a proper `FlatFileItemReader<T>` to read the data in in a paging fashion.
* create a `JdbcBatchItemWriterBuilder<T>` to write the CSV data to the DB with an upsert. A quick digression to appreciate the beauty of ye `ole "upsert"

## part 3

...was just me faffing about trying to figure out how to analyze the data and get it into a separate table. it was a huge flop. so let's not do that.

## part 4

* introduce step outcomes and conditional flows - do this _if_ that, etc.
* if an error occurs, go to an error handling step, otherwise go to the year and platform report step. 
* the year platform step is a `Tasklet` that updates a new table, that serves as sort of a view, with the new data. this data should contain the amount of sales each platform did by year.
* then set the stage for distribution and concurrence. i could do things on the same node. i should maybe show how to use an `Executor` or something? but what i really want to do is introduce remote chunking.
* have a dicusssion around remote chunking vs partitioning
* maybe to make this a little more realistic i could have another microservice that renders an `.svg` drawing a graph of of how much each platform made in a given year?
* in order to understand Spring Batch's remote chunking we need a _very_ quick discussion of Spring Integration and channels. Show a quick AMQP example that sends records from one node to another. Or maybe it just uses Apache Kafka?
* setup the final step, to read the data from the year platform report table with a reader, convert it to JSON with a processor, and then send it using a chunk itemwriter out over a `MessageChannel`.
* setup `MessagingTemplate`, `ChunkMessageChannelItemWriter<T>`, the two outbound and inbound `IntegrationFlow`s, etc.
* setup a new module, worker node, that has the spring batch integration support and spring integration but does not necessarily have spring batch
* setup the dummy item ItemProcessor and ItemWriters on the worker node.

## part 5-8
these are all about hacking on the autoconfiguration and then integrating it into the demo.

## part 9
* integrate the graalvm aot hints and profit