package kz.test.pressure;



import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.FileWriter;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.logging.Logger;


public abstract class Main extends Configured implements Tool {
	public static Integer iter = 0;
	public static Integer P = 4; //TODO
	public static Integer N = 32, M = 10000;
	public static Integer nN = N / P;
	public static Instant before;
	public static Instant after;
	public static long gap;

	public static void main(String[] args) {
		Options options = new Options();
		options.addOption("P", true, "number of splits");
		options.addOption("N", true, "size of cube");
		options.addOption("M", true, "value of number M in task");
		CommandLineParser parser = new GnuParser();
		try {
			CommandLine cmd = parser.parse( options, args);
			if(cmd.hasOption("P")) {
				P = Integer.parseInt(cmd.getOptionValue("P"));
			}
			if(cmd.hasOption("N")) {
				N = Integer.parseInt(cmd.getOptionValue("N"));
			}
			if(cmd.hasOption("M")) {
				M = Integer.parseInt(cmd.getOptionValue("M"));
			}

		} catch (ParseException e) {
			e.printStackTrace();
		}

		if (N * N / M <= 1 / 6 && N%P==0) {

		try {

				int rIter = 0;
				before = Instant.now();


				ToolRunner.run(new Configuration(), new Init(), args);

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
				fw.write("P= " + P + "\t N= " + N + "\t M= "+M+"\t time_gap= " + gap + "\n");
				fw.close();

				System.exit(rIter);
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		else {
			final Logger logger = Logger.getLogger(Main.class.getName());
			logger.info("P= "+P+" N= "+N+" M= "+M+" \nN*N/M > 1/6 or N is not multiple by P");
		}
	}

}
