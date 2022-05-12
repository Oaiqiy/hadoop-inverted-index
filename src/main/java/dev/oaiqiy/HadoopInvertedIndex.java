package dev.oaiqiy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class HadoopInvertedIndex {
    static class Map extends Mapper<Object,Text,Text, IntWritable>{
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            StringTokenizer stringTokenizer = new StringTokenizer(value.toString());

            while (stringTokenizer.hasMoreTokens()){
                IntWritable v = new IntWritable(1);
                Text k = new Text(stringTokenizer.nextToken()+"--->"+split.getPath().toString());

                context.write(k,v);
            }
        }
    }

    static class Combine extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int sum = 0;

            for(IntWritable value: values)
                sum += value.get();

            context.write(key,new IntWritable(sum));

        }
    }

    static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

            int sum = 0;

            for(IntWritable value: values)
                sum += value.get();

            context.write(key,new IntWritable(sum));

        }
    }

    static class SecondMap extends Mapper<Text,Text,Text,Text>{

        @Override
        protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

            String keyString = key.toString();

            String[] keys = keyString.split("--->");

            context.write(new Text(keys[0]),new Text(keys[1]+":"+value));
        }

    }

    static class SecondReduce extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

            StringBuilder sb = new StringBuilder();

            for(var value : values)
                sb.append(value.toString()).append(';');

            context.write(key, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        Job firstJob = Job.getInstance(conf);
        firstJob.setJarByClass(HadoopInvertedIndex.class);

        firstJob.setMapperClass(HadoopInvertedIndex.Map.class);
        firstJob.setMapOutputKeyClass(Text.class);
        firstJob.setMapOutputValueClass(IntWritable.class);

        firstJob.setCombinerClass(HadoopInvertedIndex.Combine.class);

        firstJob.setReducerClass(HadoopInvertedIndex.Reduce.class);
        firstJob.setOutputKeyClass(Text.class);
        firstJob.setOutputValueClass(IntWritable.class);


        FileInputFormat.addInputPath(firstJob, new Path("hdfs://localhost:9000/input"));
        FileOutputFormat.setOutputPath(firstJob, new Path("hdfs://localhost:9000/mid"));


        Job secondJob = Job.getInstance(conf);
        secondJob.setJarByClass(HadoopInvertedIndex.class);

        secondJob.setMapperClass(SecondMap.class);
        secondJob.setMapOutputKeyClass(Text.class);
        secondJob.setMapOutputValueClass(Text.class);

        secondJob.setReducerClass(SecondReduce.class);
        secondJob.setOutputKeyClass(Text.class);
        secondJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(secondJob, new Path("hdfs://localhost:9000/mid"));
        FileOutputFormat.setOutputPath(secondJob, new Path("hdfs://localhost:9000/output"));


        secondJob.setInputFormatClass(KeyValueTextInputFormat.class);

        ControlledJob one = new ControlledJob(firstJob.getConfiguration());
        ControlledJob two = new ControlledJob(secondJob.getConfiguration());

        one.setJob(firstJob);
        two.setJob(secondJob);



        two.addDependingJob(one);

        JobControl jobControl = new JobControl("InvertedIndex");

        jobControl.addJob(one);
        jobControl.addJob(two);


        jobControl.run();


    }
}
