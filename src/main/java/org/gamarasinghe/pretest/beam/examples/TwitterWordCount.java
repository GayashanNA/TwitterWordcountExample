package org.gamarasinghe.pretest.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang.StringUtils;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.Arrays;
import java.util.List;

/**
 */
public class TwitterWordCount {


    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);
        String inputFile =
                "/home/gayashan/projects/research/twitterwordcountbeam/src/main/resources/dataset2_medium.csv";
        String outputFile = "results/output.csv";

        PCollection<String> twitterStream = pipeline.apply("ReadTweetsAndTimestamps", TextIO.Read.from(inputFile))
                .apply("Set event time and extract tweets", ParDo.of(new SetTimestampAndExtractTextFn()))
                .apply("Tokenize Tweets Into Words", ParDo.of(new ExtractWords()))
                .apply("Apply Windowing And Triggers",
                        Window.<String>into(FixedWindows.of(Duration.standardMinutes(5)))
                                .triggering(AfterWatermark.pastEndOfWindow())
                                .withAllowedLateness(Duration.standardMinutes(2))
                                .accumulatingFiredPanes())
                .apply("Get Word count", Count.<String>perElement())
                .apply("Format the KV pairs", MapElements.via(new SimpleKeyValueFormatter()));

        twitterStream.apply("Write results to text file(s)", TextIO.Write.to(outputFile));
        pipeline.run();
    }


    private static class SetTimestampAndExtractTextFn extends DoFn<String, String> {
        @ProcessElement
        public void processElement(ProcessContext context) {
            String element = context.element();
            List<String> attributes = Arrays.asList(element.split(","));
            int length = attributes.size();
            if (length > 2) {
                String tapp = attributes.get(1);
                // get the tweets from the data elements
                // in case the tweets contain commas, join them
                String tweet = StringUtils.join(attributes.subList(3, length), ",");
                if (Character.isDigit(tapp.charAt(0))) {
                    Instant timestamp = new Instant(Long.parseLong(tapp));
                    context.outputWithTimestamp(tweet.toLowerCase(), timestamp);
                }
            }
        }
    }

    private static class ExtractWords extends DoFn<String, String> {
        @ProcessElement
        public void processElement(ProcessContext context) {
            for (String word : context.element().split("[^a-zA-Z']+")) {
                String urlRegex = "^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
                if (word.toLowerCase().matches(urlRegex) || word.isEmpty() || !Character.isAlphabetic(word.charAt(0))) {
                    if (!word.isEmpty() && (word.charAt(0) == '#' || word.charAt(0) == '@')) {
                        context.output(word.trim());
                    } else {
                        continue;
                    }
                }
                if (!word.isEmpty()) {
                    context.output(word.trim());
                }
            }
        }
    }

    /**
     *
     */
    private static class SimpleKeyValueFormatter extends SimpleFunction<KV<String, Long>, String> {
        @Override
        public String apply(KV<String, Long> input) {
            String result = input.getKey() + ":" + input.getValue();
            System.out.println(result);
            return result;
        }
    }
}
