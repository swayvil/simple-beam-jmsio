/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.solace.beam.sample;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.jms.JmsIO;
import org.apache.beam.sdk.io.jms.JmsRecord;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.joda.time.Duration;
import javax.jms.ConnectionFactory;
import java.io.IOException;

public class StreamingSimple {
    public interface Options extends PipelineOptions {
        @Description("Solace-User")
        @Default.String("default")
        String getSolaceUser();

        void setSolaceUser(String solaceUser);

        @Description("Solace-Password")
        @Default.String("default")
        String getSolacePassword();

        void setSolacePassword(String solacePassword);

        @Description("Solace-URL")
        @Default.String("amqp://localhost:5672")
        String getSolaceURL();

        void setSolaceURL(String solaceUrl);

        @Description("Solace-Word-Count-Read-Queue")
        @Default.String("SOLACE_BEAM_READ")
        String getSolaceReadQueue();

        void setSolaceReadQueue(String solaceReadQueue);
    }

    public static void main(String[] args) throws IOException {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        ConnectionFactory solaceConnectionFactory = new JmsConnectionFactory(options.getSolaceUser(), options.getSolacePassword(), options.getSolaceURL());

        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("ReadFromJms", JmsIO.read().withConnectionFactory(solaceConnectionFactory).withQueue(options.getSolaceReadQueue()))
                .apply(
                        Window.into(SlidingWindows.of(Duration.standardSeconds(30)).every(Duration.standardSeconds(5)))
                )
                .apply("TransformJmsRecordAsPojo", ParDo.of(new DoFn<JmsRecord, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        System.out.println("Message received:");
                        System.out.println(c.element().getPayload());
                        System.out.println("\n");
                        c.output(c.element().getPayload());
                    }
                }));
        PipelineResult result = pipeline.run();
        try {
            result.waitUntilFinish();
        } catch (Exception exc) {
            result.cancel();
        }
    }
}