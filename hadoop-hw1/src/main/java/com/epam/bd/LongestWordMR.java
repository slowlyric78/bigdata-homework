package com.epam.bd;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * MapReduce job analyzing text and finding longest word
 */
public class LongestWordMR {
    public static String ENABLE_DEBUG = "ENABLE_DEBUG";
    public static String STAR_LINE = "********************";

    public static class EnhancedTokenizerMapper
            extends Mapper<LongWritable, Text, IntWritable, Text>{

        private final IntWritable one = new IntWritable(1);
        private Text word = new Text();

        /**
         * Mapper implementing string tokenizer and writing longest word
         * @param key Input file position
         * @param value Text line
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            boolean enableDebug = Boolean.valueOf(context.getConfiguration().get(ENABLE_DEBUG));

            if (enableDebug) System.out.println("key: " + key.toString() + ", value: "+ value.toString());
            StringTokenizer itr = new StringTokenizer(value.toString(), " \t\n\r\f,.:;„“\"()*-");

            int maxLength = 0;
            String maxWord = null;
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();

                if (token.length() > maxLength)
                {
                    maxLength = token.length();
                    maxWord = token;
                }
            }

            if (maxWord != null) {
                word.set(maxWord);
                context.write(one, word);
            }
        }
    }

    public static class LongWordReducer
            extends Reducer<IntWritable, Text, IntWritable, Text> {
        private IntWritable maxLength = new IntWritable();
        private Text maxWord = new Text();

        /**
         * Reducer filtering candidates and picking longest word
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            boolean enableDebug = Boolean.valueOf(context.getConfiguration().get(ENABLE_DEBUG));

            if (enableDebug) System.out.println("key: " + key.toString());

            int length = 0;
            for (Text val : values) {
                if (enableDebug) System.out.println("value: " + val.toString());
                if (enableDebug) System.out.println("current length: " + length);

                if (val.getLength() > length)
                {
                    maxLength.set(val.getLength());
                    maxWord.set(val);

                    length = val.getLength();
                }
            }

            context.write(maxLength, maxWord);
        }
    }

    /**
     * Opens output file and print the result to the standard output
     * @param outputDir
     */
    private static void printLongestWord(String outputDir)
    {
        try (java.nio.file.DirectoryStream<java.nio.file.Path> directoryStream =
                     java.nio.file.Files.newDirectoryStream(
                             java.nio.file.Paths.get(outputDir), "part-r-*")) {
            for (java.nio.file.Path path : directoryStream) {
                BufferedReader reader = java.nio.file.Files.newBufferedReader(path);

                String line = reader.readLine();

                System.out.println(STAR_LINE);
                System.out.println("Result: " + line);
                System.out.println(STAR_LINE);
            }
        } catch (IOException ex) {
            System.err.println("Cannot list directory {" + outputDir + "} and print results");
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(ENABLE_DEBUG, System.getenv(ENABLE_DEBUG));
        Job job = Job.getInstance(conf, "long word search");
        job.setJarByClass(LongestWordMR.class);
        job.setMapperClass(EnhancedTokenizerMapper.class);
        job.setCombinerClass(LongWordReducer.class);
        job.setReducerClass(LongWordReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);

        if (success) {
            printLongestWord(args[1]);
        }

        System.exit(success ? 0 : 1);
    }
}
