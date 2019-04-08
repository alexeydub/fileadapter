package com.coreclass.fileadapter.task;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Moves file after job execution.
 */
@Component
public class JobCompletionNotificationListener
        extends JobExecutionListenerSupport {

    private static final Logger log = LoggerFactory
            .getLogger(JobCompletionNotificationListener.class);

    @Value("${fileadapter.output-dir}")
    private String outputDir;
    @Value("${fileadapter.error-dir}")
    private String errorDir;

    @Override
    public void afterJob(JobExecution jobExecution) {
        String fileName = jobExecution.getJobParameters()
                .getString("input.file.name");
        Path sourcePath = Paths.get(fileName);
        Path errorPath = Paths.get(errorDir);
        Path succesPath = Paths.get(outputDir);
        ExitStatus exitStatus = jobExecution.getExitStatus();
        if (ExitStatus.FAILED.getExitCode().equals(exitStatus.getExitCode())) {
            try {
                Files.move(sourcePath,
                        errorPath.resolve(sourcePath.getFileName()),
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                log.error("Error during moving " + fileName + " to " + errorPath
                        + " : " + e.getMessage());
            }
        } else if (ExitStatus.COMPLETED.getExitCode()
                .equals(exitStatus.getExitCode())) {
            try {
                Files.move(sourcePath,
                        succesPath.resolve(sourcePath.getFileName()),
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                log.error("Error during moving " + fileName + " to "
                        + succesPath + " : " + e.getMessage());
            }
        }
        log.debug("Job executed. EndTime: " + jobExecution.getEndTime() + " "
                + jobExecution.getExitStatus());
    }

}
