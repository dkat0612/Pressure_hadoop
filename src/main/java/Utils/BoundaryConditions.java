package Utils;

import java.io.IOException;

import kz.test.pressure.MRCollector;


public class BoundaryConditions {

	private Process process;
	private Integer nX, nY, nZ;
	Double u[][][];
	MathMethods mm;
	
	public BoundaryConditions(MathMethods mm, Process process, Integer nX, Integer nY, Integer nZ){
		this.nX = nX;
		this.nY = nY;
		this.nZ = nZ;
		this.process = process;
		this.mm = mm;
		u = new Double[nX + 2][nY + 2][nZ + 2];;
	}

	public void initialize(MRCollector collector) throws IOException {
		Integer i, ii, j, jj, k, kk, K = process.getRank();
		mm.initializeU(u);
		Double uu;
		for (i = 1; i <= nX; ++i) {
			ii = i + K * nX;
			for (j = 1; j <= nY; ++j)
				for (k = 1; k <= nZ; ++k) {
					collector.collectReduce(process, ii, j, k, u[i][j][k]);
				}
		}
		// when j-th's border
		j = 0;
		jj = nY + 1;
		uu = 0.0;
		for (i = 0; i <= nX + 1; ++i) {
			ii = i + K * nX;
			for (k = 0; k <= nZ + 1; ++k) {
				collector.collectReduce(process.getRank(), ii, j, k, uu);
				collector.collectReduce(process.getRank(), ii, jj, k, uu);
			}
		}
		// when i-th's border
		i = K * nX;
		ii = (K + 1) * nX + 1;
		uu = 0.0;
		for (j = 0; j <= nY + 1; ++j)
			for (k = 0; k <= nZ + 1; ++k) {
				collector.collectReduce(process.getRank(), i, j, k, uu);
				collector.collectReduce(process.getRank(), ii, j, k, uu);
			}
		// when k-th's border
		k = 0;
		kk = nZ + 1;
		uu = 0.0;
		for (i = 0; i <= nX + 1; ++i) {
			ii = i + K * nX;
			for (j = 0; j <= nY + 1; ++j) {
				collector.collectReduce(process.getRank(), ii, j, k, uu);
				collector.collectReduce(process.getRank(), ii, j, kk, uu);
			}
		}

	}
}
