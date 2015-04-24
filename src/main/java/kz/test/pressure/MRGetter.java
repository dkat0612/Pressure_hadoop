package kz.test.pressure;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;

import Utils.Process;


public class MRGetter {
	private Iterator<Text> values;
	public MRGetter(Iterator<Text> values){
		this.values = values;
	}
	public void get(Process process, Double u[][][]){
		Integer i, j, k, KK = process.getRank();
		Double U;
		while (values.hasNext()) {
			String val = values.next().toString();
			StringTokenizer itr = new StringTokenizer(val);
			i = Integer.valueOf(itr.nextToken());
			j = Integer.valueOf(itr.nextToken());
			k = Integer.valueOf(itr.nextToken());
			U = Double.valueOf(itr.nextToken());
			u[i - KK*Main.nN][j][k] = U;
		}
	}

}
