package kz.test.pressure;
import java.io.IOException;

import Utils.BoundaryConditions;
import Utils.MathMethods;
import Utils.Process;

public class Pressure {

	private Integer nX, nY, nZ;
	private Process process;
	Double u[][][];
	Double nu[][][];
	MathMethods mm;

	public Pressure(Process p, Integer nX, Integer nY, Integer nZ, Integer M) {
		this.nX = nX;
		this.nY = nY;
		this.nZ = nZ;
		process = p;
		u = new Double[nX + 2][nY + 2][nZ + 2];
		nu = new Double[nX + 2][nY + 2][nZ + 2];
		mm = new MathMethods(p.getRank(), nX, nY, nZ, M, Main.N);

	}

	public void initialize(MRCollector collector) throws IOException {
		BoundaryConditions bc = new BoundaryConditions(mm, process, nX, nY, nZ);
		bc.initialize(collector);

	}

	public void calculateOneIteration(MRGetter getter, MRCollector collector)
			throws IOException {
		Integer i, ii, j, k, KK = process.getRank();

		getter.get(process, u);

		mm.calcOneIterationU(u, nu, Main.iter);

		for (i = 1; i <= nX; ++i) {
			ii = i + KK * nX;
			for (j = 1; j <= nY; ++j)
				for (k = 1; k <= nZ; ++k) {
					collector.collectReduce(process, ii, j, k, nu[i][j][k]);
				}
		}
		for (i = 0; i <= nX + 1; ++i) {
			ii = i + KK * nX;
			for (j = 0; j <= nY + 1; ++j)
				for (k = 0; k <= nZ + 1; ++k) {
					collector.collectReduce(process.getRank(), ii, j, k,
							nu[i][j][k]);
				}
		}

	}
}
