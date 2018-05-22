/*
 * Copyright 2014 Michal Harish, michal.harish@gmail.com
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

import java.io.IOException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import io.amient.kafka.hadoop.io.KafkaInputFormat;
import io.amient.kafka.hadoop.io.MultiOutputFormat;
import io.amient.kafka.hadoop.testutils.MyJsonTimestampExtractor;
import io.amient.kafka.hadoop.testutils.SystemTestBase;

public class InvalidInputSystemTest extends SystemTestBase {

    @Test(expected = java.lang.Error.class)
    public void failsNormallyWithInvalidInput() throws IOException, ClassNotFoundException, InterruptedException {
        //configure inputs, timestamp extractor and the output path format
        KafkaInputFormat.configureKafkaTopics(conf, "topic02");
        KafkaInputFormat.configureZkConnection(conf, zkConnect);
        HadoopJobMapper.configureTimestampExtractor(conf, MyJsonTimestampExtractor.class.getName());
        MultiOutputFormat.configurePathFormat(conf, "'t={T}/d='yyyy-MM-dd'/h='HH");

        //produce and run
        simpleProducer.send(new ProducerRecord<String, String>("topic02", "1", "{invalid-json-should-fail-in-extractor"));
        runSimpleJob("topic02", "failsNormallyWithInvalidInput");
    }
}
