package kz.test.pressure;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.time.Instant;
import java.time.temporal.ChronoUnit;


public abstract class Main extends Configured implements Tool {
	public static Integer iter = 0;
	public static Integer P = 2;
	public static Integer N = 128, M = 10000;
	public static Integer nN = N / P;
	public static Instant before;
	public static Instant after;
	public static long gap;

	public static void main(String[] args){

		try {
			@SuppressWarnings("unused")
			int rInit, rIter = 0;
			before = Instant.now();


			rInit = ToolRunner.run(new Configuration(), new Init(), args);

			do {
				Job job = new Job();
				Configuration conf = job.getConfiguration();
				conf.addResource(new Path(
						Constants.CORE_SITE_XML));
				conf.addResource(new Path(
						Constants.HDFS_SITE_XML));
				FileSystem fs = FileSystem.get(conf);
				Path p = new Path(
						Constants.HDFS_192_168_1_174_9000_HDUSER_PCLASSES_V3_OUTPUT_OUT_0_OUT
								+ iter.toString() + "/_logs");
				if (fs.exists(p))
					fs.delete(p, true);
				rIter = ToolRunner.run(new Configuration(), new Iter(), args);
				++iter;
			} while (iter <= 10);
			after = Instant.now();
			gap = ChronoUnit.MILLIS.between(before, after);
			FileWriter fw = new FileWriter("/home/hadoop/time", true);
			fw.write("P= "+ P + "\t N= "+N+"\t time_gap= "+gap+"\n");
			fw.close();

			System.exit(rIter);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
