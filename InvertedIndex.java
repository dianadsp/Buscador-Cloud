//package com.mk.mapreduce;
 
 
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.jute.Index;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;
 
public class InvertedIndex {
 
    public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
 
        private final Text newKey = new Text();
        private final Text newValue = new Text("1");
 
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
 
            if (StringUtils.isBlank(value.toString())) {
                System.out.println ("línea en blanco");
                return;
            }

		String line = value.toString();
		line = line.replaceAll("[^a-zA-Z]", " ").toLowerCase();

            StringTokenizer tokenizer = new StringTokenizer(line);
            FileSplit fileInputSplit = (FileSplit) context.getInputSplit();
            String fileName = fileInputSplit.getPath().getName();
            while (tokenizer.hasMoreTokens()) {
                String word = tokenizer.nextToken();
                newKey.set(word + "\u0001" + fileName);
                context.write(newKey, newValue);
            }
        }
    }
 
    public static class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text> {
 
        private final Text newKey = new Text();
        private final Text newValue = new Text();
 
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] keys = key.toString().split("\u0001");
            List<String> list = new LinkedList<>();
            values.forEach(v -> list.add(v.toString()));
            int count = list.stream().map(Integer::valueOf).reduce(0, (s, a) -> s + a);
            newKey.set(keys[0]);
            newValue.set(keys[1] + "\u0001" + count);
            context.write(newKey, newValue);
 
        }
    }
 
    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
 
        private final Text newValue = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
 
            Map<String, Integer> map = new HashMap<>();
            for (Text v : values) {
                String[] value = v.toString().split("\u0001");
                Integer count = map.get(value[0]);
                if (Objects.isNull(count)) {
                    count = Integer.parseInt(value[1]);
                } else {
                    count = count + Integer.parseInt(value[1]);
                }
                map.put(value[0], count);
            }
 
            String info = map.entrySet().stream()
                    .sorted((a, b) -> {
                        int c = a.getValue() - b.getValue();
                        if (c != 0)
                            return c;
                        return a.getKey().compareTo(b.getKey());
                    })
                    .reduce(new StringBuilder(), (StringBuilder s, Map.Entry<String, Integer> v) -> {
                        s.append(v.getKey()).append(" ").append(v.getValue()).append(";");
                        return s;
                    }, StringBuilder::append).toString();
 
            newValue.set(info);
            context.write(key, newValue);
 
        }
    }
 
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
 
        //String uri = "hdfs://192.168.150.128:9000";
        //String input = "/invertedIndex/input";
        //String output = "/invertedIndex/output";
        Configuration conf = new Configuration();
        //if (System.getProperty("os.name").toLowerCase().contains("win"))
        //    conf.set("mapreduce.app-submission.cross-platform", "true");
 
        //FileSystem fileSystem = FileSystem.get(URI.create(uri), conf);
        //Path path = new Path(output);
        //fileSystem.delete(path, true);
 
        Job job = Job.getInstance(conf, "InvertedIndex");
        //job.setJar("./out/artifacts/hadoop_test_jar/hadoop-test.jar");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setCombinerClass(InvertedIndexCombiner.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
    	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
