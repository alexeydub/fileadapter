package com.coreclass.fileadapter.config;

import java.net.MalformedURLException;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import com.coreclass.fileadapter.task.DictEntry;
import com.coreclass.fileadapter.task.JobCompletionNotificationListener;

/**
 * Batch configuration.
 */
@Configuration
@EnableBatchProcessing
public class BatchConfig {
    
    private static final Logger log = LoggerFactory.getLogger(BatchConfig.class);
    
    @Autowired
    private StepBuilderFactory stepBuilder;

    @Autowired
    private JobBuilderFactory jobBuilder;
    
    @Bean
    @StepScope
    public FlatFileItemReader<DictEntry> reader(@Value("#{jobParameters['input.file.name']}") String fsr) throws MalformedURLException {
        log.info("incoming file: " + fsr);
        return new FlatFileItemReaderBuilder<DictEntry>()
                .name("dictionaryReader") // name
                .resource(new FileSystemResource(fsr)) // read from file
                .linesToSkip(1) // skip the header
                .delimited() // file is delimited...
                .delimiter(",") // ...by comma
                .names(new String[] { "id", "name", "value" }) // headers
                .fieldSetMapper(new BeanWrapperFieldSetMapper<DictEntry>() {
                    {
                        setTargetType(DictEntry.class);
                    }
                })
                .build();
    }

    @Bean
    public JdbcBatchItemWriter<DictEntry> writer(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<DictEntry>()
                .itemSqlParameterSourceProvider(
                        new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO dictionary (id, name, value) VALUES (:id, :name, :value)")
                .dataSource(dataSource).build();
    }

    @Bean(name="step1")
    public Step step1(ItemReader<DictEntry> reader, JdbcBatchItemWriter<DictEntry> writer)
            throws MalformedURLException {
        return stepBuilder
                .get("step1")
                .<DictEntry, DictEntry>chunk(100)
                .reader(reader)
                .writer(writer)
                .build();
    }

    @Bean
    public Job loadDictionary(JobCompletionNotificationListener listener,
            Step step1) {
        return jobBuilder.get("loadDictionary")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(step1)
                .end()
                .build();
    }
}
