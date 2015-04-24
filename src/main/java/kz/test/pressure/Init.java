package kz.test.pressure;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.*;

import Utils.Process;

public class Init extends Configured implements Tool {

    public static class MapClass extends MapReduceBase implements
            Mapper<Text, Text, Text, Text> {

        public void map(Text key, Text value,
                        OutputCollector<Text, Text> output, Reporter reporter) {
            try {
                @SuppressWarnings("resource")
			MRCollector collector = new MRCollector(output);
			collector.collectMap();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class Reduce extends MapReduceBase implements
            Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterator<Text> values,
                           OutputCollector<Text, Text> output, Reporter reporter) {
            try {
                Process process = new Process(Integer.valueOf(key.toString()), Main.P);
                Pressure pressure = new Pressure(process, Main.nN, Main.N, Main.N, Main.M);
                MRCollector collector = new MRCollector(output);
                pressure.initialize(collector);

            } catch (IOException e) {
                e.printStackTrace();
            } finally {
            }


        }
    }

    public int run(String[] args) {

        Job fs_job = null;
        try {
            fs_job = new Job();

            Configuration job_conf = fs_job.getConfiguration();
            job_conf.addResource(new Path(
                    Constants.CORE_SITE_XML));
            job_conf.addResource(new Path(
                    Constants.HDFS_SITE_XML));
            FileSystem fs = FileSystem.get(job_conf);
            Configuration conf = getConf();
            JobConf job = new JobConf(conf, Init.class);

            Path in = new Path(
                    Constants.GENERAL_PATH + "/in/input");
            Path out = new Path(
                    Constants.GENERAL_PATH + "/output/out_0");
            if (fs.exists(out))
                fs.delete(out, true);

            FileInputFormat.setInputPaths(job, in);
            FileOutputFormat.setOutputPath(job, out);
            job.setNumMapTasks(Main.P);
            job.setNumReduceTasks(Main.P);
            job.setJobName("Init");
            job.setMapperClass(MapClass.class);
            job.setReducerClass(Reduce.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setInputFormat(KeyValueTextInputFormat.class);
            job.setOutputFormat(TextOutputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            JobClient.runJob(job);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            return 0;
        }
    }

}
