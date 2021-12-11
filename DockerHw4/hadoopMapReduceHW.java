//package cs1660_intro_to_cloud_computing.DockerHw4;

import java.io.IOException;
//import java.util.StringTokenizer;
//import java.util.HashMap;
//import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class hadoopMapReduceHW {
    public static class hadoopMapReduceHWMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final int MISSING = 9999;

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String year = line.substring(15, 23);
            int airTemperature;
            if (line.charAt(87) == '+') {
                airTemperature = Integer.parseInt(line.substring(88, 92));
            } else {
                airTemperature = Integer.parseInt(line.substring(87, 92));
            }
            String quality = line.substring(92, 93);
            if (airTemperature != MISSING && quality.matches("[01459]")) {
                context.write(new Text(year), new IntWritable(airTemperature));
            }
        }
    }

    public static class hadoopMapReduceHWReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int maxValue = Integer.MIN_VALUE;
            for (IntWritable value : values) {
                maxValue = Math.max(maxValue, value.get());
            }
            context.write(key, new IntWritable(maxValue));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: hadoopMapReduceHW <input> <output>");
            System.exit(-1);
        }
        Job job = new Job();
        job.setJarByClass(hadoopMapReduceHW.class);
        job.setJobName("hadoopMapReduceHW");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(hadoopMapReduceHWMapper.class);
        job.setReducerClass(hadoopMapReduceHWReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
