package kz.test.pressure;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;

import Utils.Process;

public class MRCollector extends MapReduceBase {
	private OutputCollector<Text, Text> output;

	public MRCollector(OutputCollector<Text, Text> output) {
		this.output = output;
	}

	public Text toText(Integer x, Integer y, Integer z, Double u) {
		String v = x.toString() + " " + y.toString() + " " + z.toString() + " "
				+ u.toString();
		return new Text(v);
	}

	public void collectMap() throws IOException {
		Integer P = Main.P;
		String val = "";
		for (Integer i = 0; i < P; ++i)
			output.collect(new Text(i.toString()), new Text(val));
	}

	public void collectReduce(Process process, Integer x, Integer y, Integer z,
			Double u) throws IOException {
		Text value = toText(x, y, z, u);
		output.collect(new Text(process.getRank().toString()), value);
		if (process.getUp() != null && x == 1)
			output.collect(new Text(process.getUp().toString()), value);
		if (process.getDown() != null && x == Main.nN)
			output.collect(new Text(process.getDown().toString()), value);

	}

	public void collectReduce(Integer rank, Integer x, Integer y, Integer z,
			Double u) throws IOException {
		Text value = toText(x, y, z, u);
		output.collect(new Text(rank.toString()), value);
	}
}
