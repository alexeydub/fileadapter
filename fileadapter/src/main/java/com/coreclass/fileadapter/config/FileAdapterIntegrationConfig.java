package com.coreclass.fileadapter.config;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.integration.launch.JobLaunchingGateway;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.file.FileReadingMessageSource;
import org.springframework.integration.file.filters.SimplePatternFileListFilter;
import org.springframework.messaging.MessageChannel;

import com.coreclass.fileadapter.task.FileMessageToJobRequest;

/**
 * Integration configuration
 */
@Configuration
@EnableIntegration
@IntegrationComponentScan
public class FileAdapterIntegrationConfig {

    private static final Logger log = LoggerFactory
            .getLogger(FileAdapterIntegrationConfig.class);

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private Job job; // loadDictionary

    // input directory with *.cvs files
    @Value("${fileadapter.input-dir}")
    private String inputDir;

    @Bean
    public MessageChannel fileInputChannel() {
        return new DirectChannel();
    }

    @Bean
    public MessageSource<File> inboundDirectory() {
        File f = new File(inputDir);
        log.info("file adapter input dir: " + f.getAbsolutePath());
        FileReadingMessageSource messageSource = new FileReadingMessageSource();
        messageSource.setDirectory(new File(getInputDir()));
        messageSource.setFilter(new SimplePatternFileListFilter("*.csv"));
        messageSource.setScanEachPoll(true);
        messageSource.setUseWatchService(true);
        return messageSource;
    }

    @Bean
    public FileMessageToJobRequest fileMessageToJobRequest() {
        FileMessageToJobRequest request = new FileMessageToJobRequest();
        request.setFileName("input.file.name");
        request.setJob(job);
        return request;
    }

    @Bean
    public JobLaunchingGateway jobLauncherHandler() {
        SimpleJobLauncher launcher = new SimpleJobLauncher();
        launcher.setJobRepository(jobRepository);
        launcher.setTaskExecutor(new SyncTaskExecutor());
        return new JobLaunchingGateway(launcher);
    }

    @Bean
    public IntegrationFlow fileMover() {
        return IntegrationFlows
                .from(inboundDirectory(),
                        c -> c.poller(Pollers.fixedDelay(10000)))
                .log("Get a new file")
                .channel(fileInputChannel())
                .log("Creating a job request")
                .handle(fileMessageToJobRequest()).log("toJobRequest")
                .log("Launch a job")
                .handle(jobLauncherHandler()).log("launcher")
                .log("end").get();
    }

    public String getInputDir() {
        return inputDir;
    }

    public void setInputDir(String inputDir) {
        this.inputDir = inputDir;
    }

}
