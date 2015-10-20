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

public class ValueHistogram {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            String arr[] = value.toString().split("\\r?\\n");
            for (String row: arr) {
                if (row.startsWith("\"")) {
                    continue;
                }
                String parts[] = row.split(",");
                output.collect(new IntWritable(new Integer(parts[1])), new Text(parts[4]));
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            HashMap<String, Integer> countries_map = new HashMap<String, Integer>();
            ArrayList<Integer> counties = new ArrayList<>();
            String cp = new String();
            
            while (values.hasNext()) {
                cp = values.next().toString();
                if (countries_map.containsKey(cp)) {
                    countries_map.put(cp, countries_map.get(cp) + 1);
                }
                else {
                    countries_map.put(cp, 1);   
                }
            }

            for (java.util.Map.Entry<String, Integer> entry : countries_map.entrySet()) {
                counties.add(entry.getValue());
            }
            output.collect(key, new Text("" + countries_map.entrySet().size() + " " + Collections.min(counties) + " "
             + median(counties) + " " + Collections.max(counties) + " " + mean(counties) + " " + standard_deviation(counties)));
        }

        private int median(ArrayList<Integer> l) {
            Collections.sort(l);
            int t = l.size();
            return l.get(t / 2);
        }

        private float mean(ArrayList<Integer> l) {
            int t = l.size();
            Integer sum = new Integer(0);
            for (Integer i: l) {
                sum += i;
            }
            return ((float) sum) / t;
        }

        private float standard_deviation(ArrayList<Integer> l) {
            int t = l.size();
            float ans = 0, mn = this.mean(l);

            for (Integer i: l) {
                ans += (i - mn) * (i - mn);
            }
            return (float)Math.sqrt(ans / (t - 1));
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(ValueHistogram.class);
        conf.setJobName("ValueHistogram");

        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
