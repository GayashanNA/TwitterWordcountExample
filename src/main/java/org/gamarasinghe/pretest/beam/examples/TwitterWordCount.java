package org.gamarasinghe.pretest.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
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
        @Default.String("/home/gayashan/projects/research/twitterwordcountbeam/src/main/resources/dataset2_medium.csv")
        String getInputFile();

        void setInputFile(String value);

        @Description("Path of the file to write to")
        @Default.String("results/twitter-wordcount-output")
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
        // 7. Format the output per each word-count to be printed/displayed.
        PCollection<String> twitterStream = pipeline
                .apply("ReadTweetsAndTimestamps", TextIO.Read.from(inputFile))
                .apply("Set event time and extract tweets", ParDo.of(new SetTimestampAndExtractTextFn()))
                .apply("Tokenize Tweets Into Words", ParDo.of(new ExtractWords()))
                .apply("Apply Windowing And Triggers",
                        Window.<String>into(FixedWindows.of(Duration.standardMinutes(10)))
                                .triggering(AfterWatermark.pastEndOfWindow()
                                        .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
                                                .plusDelayOf(Duration.standardMinutes(3)))
                                        .withLateFirings(AfterPane.elementCountAtLeast(1)))
                                .withAllowedLateness(Duration.standardMinutes(4))
                                .accumulatingFiredPanes())
                .apply("Get Word count", Count.<String>perElement())
                .apply("Create Window to wordcount mapping", ParDo.of(new SetWordcountWindowMap()))
                .apply("Groupby key in each window", GroupByKey.<IntervalWindow, KV<String, Long>>create())
                .apply("Format wordcount per window to String", ParDo.of(new FormatWordcountPerWindow()));

        // 8. Write the results to file(s)
        twitterStream.apply("Write results to text file(s)", TextIO.Write.to(outputFile));

        // 9. Execute the pipeline that you have constructed
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
                // in case the tweets contain commas, they will be separated by the previous split. So join them.
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

    private static class FormatWordcountPerWindow extends DoFn<KV<IntervalWindow, Iterable<KV<String, Long>>>, String> {
        @ProcessElement
        public void processElement(ProcessContext context) {
            System.out.println(context.element());
            for (KV<String, Long> wordcount : context.element().getValue()) {
                String wordcountOutput = wordcount.getKey() + ":" + wordcount.getValue();
                context.output(wordcountOutput);
            }
        }
    }

    private static class SetWordcountWindowMap extends DoFn<KV<String, Long>, KV<IntervalWindow, KV<String, Long>>> {
        @ProcessElement
        public void processElement(ProcessContext context, IntervalWindow window) {
            context.output(KV.of(window, context.element()));
        }
    }
}
