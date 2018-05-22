/*
 * Copyright 2012 Michal Harish, michal.harish@gmail.com
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.amient.kafka.hadoop;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.LoggerFactory;

import io.amient.kafka.hadoop.io.KafkaInputFormat;
import io.amient.kafka.hadoop.io.MultiOutputFormat;

public class HadoopJob extends Configured implements Tool {

    private final static org.slf4j.Logger LOG = LoggerFactory.getLogger(HadoopJob.class);

    static {
        Configuration.addDefaultResource("core-site.xml");
    }

    @Override
    public int run(String[] args) throws Exception {
        CommandLineParser parser = new DefaultParser();
        Options options = buildOptions();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("h") || cmd.getArgs().length == 0) {
            printHelpAndExit(options);
        }
        String hdfsPath = cmd.getArgs()[0];
        Configuration conf = getConf();
        conf.setBoolean("mapred.map.tasks.speculative.execution", false);
        conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
        if (cmd.hasOption("topics")) {
            LOG.info("Using topics: " + cmd.getOptionValue("topics"));
            KafkaInputFormat.configureKafkaTopics(conf, cmd.getOptionValue("topics"));
        } else {
            printHelpAndExit(options);
        }

        KafkaInputFormat.configureZkConnection(conf, cmd.getOptionValue("zk-connect", "localhost:2181"));
        if (cmd.hasOption("consumer-group")) {
            CheckpointManager.configureUseZooKeeper(conf, cmd.getOptionValue("consumer-group", "dev-hadoop-loader"));
        }

        if (cmd.getOptionValue("autooffset-reset") != null) {
            KafkaInputFormat.configureAutoOffsetReset(conf, cmd.getOptionValue("autooffset-reset"));
        }

        JobConf jobConf = new JobConf(conf);
        if (cmd.hasOption("remote")) {
            String ip = cmd.getOptionValue("remote");
            LOG.info("Default file system: hdfs://" + ip + ":8020/");
            jobConf.set("fs.defaultFS", "hdfs://" + ip + ":8020/");
            LOG.info("Remote jobtracker: " + ip + ":8021");
            jobConf.set("mapred.job.tracker", ip + ":8021");
        }

        Path jarTarget = new Path(
                getClass().getProtectionDomain().getCodeSource().getLocation()
                        + "../kafka-hadoop-loader.jar"
        );

        if (new File(jarTarget.toUri()).exists()) {
            // running from IDE/ as maven
            jobConf.setJar(jarTarget.toUri().getPath());
            LOG.info("Using target jar: " + jarTarget.toString());
        } else {
            // running from jar remotely or locally
            jobConf.setJarByClass(getClass());
            LOG.info("Using parent jar: " + jobConf.getJar());
        }


        Job job = Job.getInstance(jobConf, HadoopJob.class.getName());

        job.setInputFormatClass(KafkaInputFormat.class);
        job.setMapperClass(HadoopJobMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(MultiOutputFormat.class);
        job.setNumReduceTasks(0);

        MultiOutputFormat.setOutputPath(job, new Path(hdfsPath));
        MultiOutputFormat.setCompressOutput(job, cmd.getOptionValue("compress-output", "on").equals("on"));

        LOG.info("Output hdfs location: {}", hdfsPath);
        LOG.info("Output hdfs compression: {}", MultiOutputFormat.getCompressOutput(job));

        return job.waitForCompletion(true) ? 0 : -1;
    }

    private void printHelpAndExit(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("kafka-hadoop-loader.jar", options);
        System.exit(0);
    }

    private Options buildOptions() {
        Options options = new Options();

        // @formatter:off
        options.addOption(Option.builder("t")
        		.argName("topics")
                .longOpt("topics")
                .hasArg()
                .desc("kafka topics")
                .build());

        options.addOption(Option.builder("g")
        		.argName("groupid")
                .longOpt("consumer-group")
                .hasArg()
                .desc("kafka consumer groupid")
                .build());

        options.addOption(Option.builder("z")
        		.argName("zk")
                .longOpt("zk-connect")
                .hasArg()
                .desc("ZooKeeper connection String")
                .build());

        options.addOption(Option.builder("o")
        		.argName("offset")
                .longOpt("offset-reset")
                .hasArg()
                .desc("Reset all offsets to either 'earliest' or 'latest'")
                .build());

        options.addOption(Option.builder("c")
        		.argName("compression")
                .longOpt("compress-output")
                .hasArg()
                .desc("GZip output compression on|off")
                .build());

        options.addOption(Option.builder("r")
        		.argName("ip_address")
                .longOpt("remote")
                .hasArg()
                .desc("Running on a remote hadoop node")
                .build());

        options.addOption(Option.builder("h")
                .longOpt("help")
                .desc("Show this help")
                .build());
     // @formatter:on
        return options;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new HadoopJob(), args);
		System.exit(exitCode);
	}

}
