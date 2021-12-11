package cs1660_intro_to_cloud_computing.Project2;

import java.io.IOException;
//import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopN {

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        config.set("N", args[2]);
        Job job = Job.getInstance(config, "TopN");
        job.setJarByClass(TopN.class);
        job.setMapperClass(TNMapper.class);
        job.setReducerClass(TNReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

    public static class TNMapper extends Mapper<Object, Text, Text, IntWritable> {
        private int objectRetNum;
        private TreeMap<Integer, String> frequencyMap;

        public void mapSetup(Context context) {
            objectRetNum = Integer.parseInt(context.getConfiguration().get("N"));
            frequencyMap = new TreeMap<Integer, String>();
        }

        public void map(Object key, Text value, Context context) {

            int totalCount = 0;

            // ArrayList<String> lineWord = new ArrayList<String>();
            // ArrayList<String> lineCount = new ArrayList<String>();
            // for(String line : value.toString()){
            // String[] pair = line.split("\\s+");
            String[] mapPair = value.toString().split("\\s+");
            String lineWord = mapPair[0];
            // lineWord.add(pair[0]);
            // pair[1].split(",");
            // }
            String[] count = mapPair[1].split(",");

            for (String wordCount : count) {
                String countNum = wordCount.split(":")[1];
                totalCount += Integer.parseInt(countNum);
            }
            frequencyMap.put(Integer.valueOf(totalCount), lineWord);
            if (frequencyMap.size() > objectRetNum)
                frequencyMap.remove(frequencyMap.firstKey());
        }

        public void reset(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer, String> mapEntry : frequencyMap.entrySet()) {
                context.write(new Text(mapEntry.getValue()), new IntWritable(mapEntry.getKey()));
            }
        }
    }

    public static class TNReducer extends Reducer<Text, IntWritable, IntWritable, Text> {
        private int valueRetNum;
        private TreeMap<Integer, String> FrequencyReducer;

        public void mapSetup(Context context) {
            valueRetNum = Integer.parseInt(context.getConfiguration().get("N"));
            FrequencyReducer = new TreeMap<Integer, String>();
        }

        public void reduce(Text key, Iterable<IntWritable> value, Context context) {
            int totalCount = 0;
            for (IntWritable wordValue : value) {
                totalCount = wordValue.get();
            }
            FrequencyReducer.put(totalCount, key.toString());
            if (FrequencyReducer.size() > valueRetNum)
                FrequencyReducer.remove(FrequencyReducer.firstKey());
        }

        public void reset(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer, String> reduceEntry : FrequencyReducer.entrySet()) {
                context.write(new IntWritable(reduceEntry.getKey()), new Text(reduceEntry.getValue()));
            }
        }
    }
}
