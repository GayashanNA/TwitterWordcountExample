package org.gamarasinghe.pretest.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
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

    /***
     * Custom pipeline configuration options to get the input file name and the output file name.
     * In case these values are not given via the command line arguments, the default values will be used.
     */
    public interface TwitterWordCountOptions extends PipelineOptions {
        @Description("Path of the file to read from")
        @Default.String("/home/gayashan/projects/research/twitterwordcountbeam/src/main/resources/dataset2_small.csv")
        String getInputFile();

        void setInputFile(String value);

        @Description("Path of the file to write to")
        @Default.String("results/twitter-wordcount-output.txt")
        String getOutputFile();

        void setOutputFile(String value);
    }

    public static void main(String[] args) {
        // Creating the pipeline using my custom configuration options
        TwitterWordCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as
                (TwitterWordCountOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        // Get the input and output file names from the configuration options
        String inputFile = options.getInputFile();
        String outputFile = options.getOutputFile();

        // Constructing the pipeline workflow
        // 1. Read the tweet stream from the csv file
        // 2. Extract the timestamp from the csv file and attach the timestamp to each event
        // 3. Extract the tweets from the csv file and put it in the event
        // 4. Tokenize tweets in to words
        // 5. Apply windowing to the stream and add an event time based trigger
        // 6. Count the words based on each window (results in a key-value pair of word-count)
        // 7. Combine the counts per key per window
        // 8. Format the output per each word-count to be printed/displayed.
        PCollection<String> twitterStream = pipeline
                .apply("Read Tweets And Timestamps", TextIO.Read.from(inputFile))
                .apply("Set event time and extract tweets", ParDo.of(new SetTimestampAndExtractTextFn()))
                .apply("Tokenize Tweets Into Words", ParDo.of(new ExtractWords()))
                .apply("Apply Windowing And Triggers",
                        Window.<String>into(FixedWindows.of(Duration.standardMinutes(5)))
                                .triggering(AfterWatermark.pastEndOfWindow()
                                        .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
                                                .plusDelayOf(Duration.standardMinutes(1)))
                                        .withLateFirings(AfterPane.elementCountAtLeast(1)))
                                .withAllowedLateness(Duration.standardMinutes(5))
                                .discardingFiredPanes())
                .apply("Get Word count", Count.<String>perElement())
                .apply("Group by key and Combine the counts per window",
                        Combine.<String, Long, Long>perKey(Sum.ofLongs()))
                .apply("Format wordcount per window to String", ParDo.of(new FormatWordcountPerWindow()));

        // 9. Write the results to file(s)
        twitterStream.apply("Write results to text file(s)", TextIO.Write.to(outputFile));

        // Execute the pipeline that you have constructed
        pipeline.run();
    }

    /**
     * Extract the timestamp from the csv file and assign it to each event
     * And extract the tweets from the file and emit it as the content of the event
     */
    private static class SetTimestampAndExtractTextFn extends DoFn<String, String> {
        @ProcessElement
        public void processElement(ProcessContext context) {
            String element = context.element();
            List<String> attributes = Arrays.asList(element.split(","));
            int length = attributes.size();
            if (length > 2) {
                String tapp = attributes.get(1);
                // get the tweets from the data elements
                // in case the tweets contain commas, they will be separated by the previous split. So join them.
                String tweet = StringUtils.join(attributes.subList(3, length), ",");
                if (Character.isDigit(tapp.charAt(0))) {
                    Instant timestamp = new Instant(Long.parseLong(tapp));
                    context.outputWithTimestamp(tweet.toLowerCase(), timestamp);
                }
            }
        }
    }

    /**
     * Tokenize the tweet stream in to words
     */
    private static class ExtractWords extends DoFn<String, String> {
        @ProcessElement
        public void processElement(ProcessContext context) {
            for (String word : context.element().split("[^a-zA-Z']+")) {
                if (!word.isEmpty()) {
                    context.output(word.trim());
                }
            }
        }
    }

    /**
     * Format the wordcount key values in to printable string
     */
    private static class FormatWordcountPerWindow extends DoFn<KV<String, Long>, String> {
        @ProcessElement
        public void processElement(ProcessContext context) {
            String result = context.element().getKey() + ":" + context.element().getValue();
            System.out.println(result);
            context.output(result);
        }
    }

}
