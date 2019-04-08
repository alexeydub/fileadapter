package com.coreclass.fileadapter.task;

import java.io.File;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.integration.launch.JobLaunchRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.integration.annotation.Transformer;
import org.springframework.messaging.Message;

/**
 * Provides transformer to create a job request.
 */
public class FileMessageToJobRequest {

    @Autowired
    private Job job;
    private String fileName;

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public void setJob(Job job) {
        this.job = job;
    }

    @Transformer
    public JobLaunchRequest toRequest(Message<File> message) {
        JobParametersBuilder builder = new JobParametersBuilder();
        builder.addString(fileName, message.getPayload().getAbsolutePath());

        return new JobLaunchRequest(job, builder.toJobParameters());
    }
}
