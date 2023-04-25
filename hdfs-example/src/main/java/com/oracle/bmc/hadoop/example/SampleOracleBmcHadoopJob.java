/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hadoop.example;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oracle.bmc.hdfs.BmcFilesystem;

public class SampleOracleBmcHadoopJob {
    private static final String SAMPLE_JOB_PATH = "/samplehadoopjob";
    private static final String INPUT_FILE = SAMPLE_JOB_PATH + "/input.dat";
    private static final String OUTPUT_DIR = SAMPLE_JOB_PATH + "/output";

    // non-static since this is the runner class it needs to initialize after we set the properties
    private final Logger log = LoggerFactory.getLogger(SampleOracleBmcHadoopJob.class);

    /**
     * Runner for sample hadoop job. This expects 3 args: path to configuration file, Object Store namespace, Object
     * Store bucket. To run this, you must:
     * <ol>
     * <li>Create a standard hadoop configuration file</li>
     * <li>Create the bucket ahead of time.</li>
     * </ol>
     * This runner will create a test input file in a file '/samplehadoopjob/input.dat', and job results will be written
     * to '/samplehadoopjob/output'.
     *
     * @param args
     *            1) path to configuration file, 2) namespace, 3) bucket
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException(
                    "Must have 3 args: 1) path to config file, 2) object storage namespace, 3) object storage bucket");
        }

        final SampleOracleBmcHadoopJob job =
                new SampleOracleBmcHadoopJob(args[0], args[1], args[2]);
        System.exit(job.execute());
    }

    private final String configurationFilePath;
    private final String namespace;
    private final String bucket;

    public SampleOracleBmcHadoopJob(String configurationFilePath, String namespace, String bucket) {
        this.configurationFilePath = configurationFilePath;
        this.namespace = namespace;
        this.bucket = bucket;

        if (!new File(configurationFilePath).exists()) {
            throw new IllegalArgumentException(
                    "File '" + configurationFilePath + "' does not exist");
        }
    }

    public int execute()
            throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        log.info("Creating hadoop configuration");
        final Configuration configuration = this.createConfiguration(this.configurationFilePath);

        final String authority = this.bucket + "@" + this.namespace;
        final String uri = "oci://" + authority;
        log.info("Using uri: {}", uri);

        log.info("Creating job inputs");
        this.setup(uri, configuration);

        log.info("Creating job");
        final Job job = this.createJob(configuration);

        final String in = uri + INPUT_FILE;
        final String out = uri + OUTPUT_DIR;
        log.info("Using input: {}", in);
        log.info("Using output: {}", out);

        FileInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        log.info("Executing job...");
        final int response = job.waitForCompletion(true) ? 0 : 1;

        log.info("Attempting to read job results");
        this.tryReadResult(uri, configuration);
        return response;
    }

    private Configuration createConfiguration(final String configFilePath) {
        final Configuration configuration = new Configuration();
        configuration.addResource(new Path(configFilePath));
        return configuration;
    }

    private void setup(final String uri, final Configuration configuration)
            throws IOException, URISyntaxException {
        try (final BmcFilesystem fs = new BmcFilesystem()) {
            fs.initialize(new URI(uri), configuration);
            fs.delete(new Path(SAMPLE_JOB_PATH), true);
            final FSDataOutputStream output = fs.create(new Path(INPUT_FILE));
            output.writeChars("foo\nbar\ngak\ntest\nfoo\ngak\n\ngak");
            output.close();
        }
    }

    private Job createJob(final Configuration configuration) throws IOException {
        final Job job = Job.getInstance(configuration, "word count");
        job.setJarByClass(SampleOracleBmcHadoopJob.class);
        job.setMapperClass(SimpleMapper.class);
        job.setCombinerClass(SimpleReducer.class);
        job.setReducerClass(SimpleReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job;
    }

    private void tryReadResult(final String uri, final Configuration configuration)
            throws IOException, URISyntaxException {
        try (final BmcFilesystem fs = new BmcFilesystem()) {
            fs.initialize(new URI(uri), configuration);
            // this should be the output file name, but that could change
            final FSDataInputStream input = fs.open(new Path(OUTPUT_DIR + "/part-r-00000"));

            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            IOUtils.copy(input, baos);
            log.info("\n=====\n" + baos.toString() + "=====");
            input.close();
        }
    }
}
