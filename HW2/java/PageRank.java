package org.myorg;

import java.io.IOException;
import java.util.*;
import java.util.Collections;
import java.util.Map;
import java.lang.Math;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class PageRank {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
        
        @Override
        public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            String arr[] = value.toString().split("\\r?\\n");
            for (String row: arr) {
                if (row.equals("")) {
                    continue;
                }

                String[] parts = row.split("\t");
                if (parts.length == 3) {    
                    String[] edges = parts[2].split(" ");
                    Double cpg = Double.parseDouble(parts[1]);
                    for (String edg: edges) {
                        output.collect(new IntWritable(new Integer(edg)), new Text((cpg / edges.length) + "\t"));
                    }
                    output.collect(new IntWritable(new Integer(parts[0])), new Text("\t" + parts[2]));
                }
                else{
                    output.collect(new IntWritable(new Integer(parts[0])), new Text("\t"));
                }
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
        
        @Override
        public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            Double cpg = new Double(0);
            String to = "";
            int cp = 0;
            while (values.hasNext()) {
                ++cp;
                String st = values.next().toString();
                if (!st.equals("\t") && st.endsWith("\t")) {
                    cpg += Double.parseDouble(st.substring(0, st.length() - 1));
                }
                else {
                    to = st.substring(1);
                }
            }

            cpg *= 0.85;
            cpg += 0.15;

            output.collect(key, new Text(cpg + "\t" + to));
        }
    }

    public static void main(String[] args) throws Exception {
        for (int i = 1; i <= 30; ++i) {
            JobConf conf = new JobConf(PageRank.class);
            conf.setJobName("PageRank_step_" + i);

            conf.setOutputKeyClass(IntWritable.class);
            conf.setOutputValueClass(Text.class);

            conf.setMapperClass(Map.class);
            conf.setReducerClass(Reduce.class);

            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);
            conf.setNumReduceTasks(3);

            if (i == 1) {
                FileInputFormat.setInputPaths(conf, new Path(args[0]));
                FileOutputFormat.setOutputPath(conf, new Path(args[1] + "/step_1"));
            }
            else {
                FileInputFormat.setInputPaths(conf, new Path(args[1] + "/step_" + (i - 1)));
                FileOutputFormat.setOutputPath(conf, new Path(args[1] + "/step_" + i));   
            }

            JobClient.runJob(conf);
        }
    }
}
