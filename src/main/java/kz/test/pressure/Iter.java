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

public class Iter extends Configured implements Tool {

	public static class MapClass extends MapReduceBase implements
			Mapper<Text, Text, Text, Text> {

		public void map(Text key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			output.collect(key, value);

		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			Process process = new Process(Integer.valueOf(key.toString()),
					Main.P);
			Pressure pressure = new Pressure(process, Main.nN, Main.N, Main.N,
					Main.M);
			MRCollector collector = new MRCollector(output);
			MRGetter getter = new MRGetter(values);
			pressure.calculateOneIteration(getter, collector);
		}
	}

	public int run(String[] args){

		try {
			Job job1 = new Job();
			Configuration conf1 = job1.getConfiguration();
			conf1.addResource(new Path(
                    Constants.CORE_SITE_XML));
			conf1.addResource(new Path(
                    Constants.HDFS_SITE_XML));
			FileSystem hdfs = FileSystem.get(conf1);
			conf1 = getConf();

			JobConf job = new JobConf(conf1, Iter.class);
			Path in, out;

			job.setNumMapTasks(Main.P);
			job.setNumReduceTasks(Main.P);

			job.setJobName("Iter");
			job.setMapperClass(MapClass.class);
			job.setReducerClass(Reduce.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setInputFormat(KeyValueTextInputFormat.class);
			job.setOutputFormat(TextOutputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			Integer IN = Main.iter, OUT = Main.iter + 1;

			in = new Path(
                    Constants.HDFS_192_168_1_174_9000_HDUSER_PCLASSES_V3_OUTPUT_OUT
                            + IN.toString());
			out = new Path(
                    Constants.HDFS_192_168_1_174_9000_HDUSER_PCLASSES_V3_OUTPUT_OUT
                            + OUT.toString());
			if (hdfs.exists(out))
                hdfs.delete(out, true);
			FileInputFormat.setInputPaths(job, in);
			FileOutputFormat.setOutputPath(job, out);

			JobClient.runJob(job);

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			return 0;
		}

	}

}
