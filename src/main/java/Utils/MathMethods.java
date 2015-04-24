package Utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

public class MathMethods {
	private final double L = 1;
	private Integer nX, nY, nZ, M;
	private double delta_x;
	private double delta_t;
	static final double pi = Math.acos(-1.0);
	static final double a = pi, b = 3 * pi * pi;
	private double v;
	double x[];
	double y[];
	double z[];
	private Integer rank;

	public static double fK(double x, double y, double z) {
		return 0.5 * Math.cos(Math.pow(x, 2) * Math.pow(y, 2) * Math.pow(z, 2));
	}

	public static double fF(double t, double x, double y, double z, double K) {
		return Math.exp(-b * t) * Math.sin(a * x) * Math.sin(a * y)
				* Math.sin(a * z) * K;
	}

	public static double fu(double x, double y, double z) {
		return Math.sin(a * x) * Math.sin(a * y) * Math.sin(a * z);
	}

	public double fcoord(int ind) {
		return (Math.round((ind * delta_x) * 1000) / 1000.0);
	}

	public MathMethods(Integer rank, Integer nX, Integer nY, Integer nZ,
			Integer M, Integer NX) {
		this.rank = rank;
		this.nX = nX;
		this.nY = nY;
		this.nZ = nZ;
		this.M = M;
		delta_x = L / NX;
		delta_t = 0.15 * delta_x * delta_x;
		v = delta_t / (delta_x * delta_x);
		x = new double[nX + 2];
		y = new double[nY + 2];
		z = new double[nZ + 2];
		initXYZ();
		
	}

	private void initXYZ() {
		Integer i, ii;
		for (i = 1; i <= nX + 1; ++i) {
			ii = i + rank * nX;
			x[i] = fcoord(ii);
		}
		for (i = 1; i <= nY + 1; ++i) {
			y[i] = fcoord(i);
		}
		for (i = 1; i <= nZ + 1; ++i) {
			z[i] = fcoord(i);
		}
	}

	public void initializeU(Double[][][] u) throws FileNotFoundException {
		PrintWriter pw = new PrintWriter(new File("test.txt"));
		for (Integer i = 1; i <= nX; ++i) {
			for (Integer j = 1; j <= nY; ++j)
				for (Integer k = 1; k <= nZ; ++k) {
					u[i][j][k] = fu(x[i], y[j], z[k]);
					pw.write(x[i]+" "+y[j]+" "+z[k]+"\n");
				}
		}
		pw.close();
	}

	public void calcT(Double[] t) {
		for (int i = 0; i <= M; i++) {
			t[i] = i * delta_t;
		}
	}

	public void calcK(Double K[][][]) {
		int i, j, k;
		for (i = 0; i <= nX + 1; ++i)
			for (j = 0; j <= nY + 1; ++j)
				for (k = 0; k <= nZ + 1; ++k) {
					K[i][j][k] = fK(x[i], y[j], z[k]);
				}
	}

	public void calcF(Double f[][][], Double t[], Double K[][][], Integer iter) {
		int i, j, k;
		for (i = 1; i <= nX; ++i)
			for (j = 1; j <= nY; ++j)
				for (k = 1; k <= nZ; ++k) {
					f[i][j][k] = fF(t[iter + 1], x[i], y[j], z[k],
							K[i][j][k]);
				}
	}

	public void calcOneIterationU(Double[][][] u, Double[][][] nu, Integer iter) {
		Integer i, j, k;
		Double f[][][] = new Double[nX + 2][nY + 2][nZ + 2];
		Double K[][][] = new Double[nX + 2][nY + 2][nZ + 2];
		Double t[] = new Double[M + 1];
		Double KF, KB;
		calcT(t);
		calcK(K);
		calcF(f, t, K, iter);
		for (i = 1; i <= nX; ++i) {
			for (j = 1; j <= nY; ++j)
				for (k = 1; k <= nZ; ++k) {
					KF = 0.5 * (K[(i + 1)][j][k] + K[i][j][k]);
					KB = 0.5 * (K[i][j][k] + K[(i - 1)][j][k]);
					nu[i][j][k] = v
							* (KF * (u[i + 1][j][k] - u[i][j][k]) - KB
									* (u[i][j][k] - u[i - 1][j][k]))
							+ v
							* (KF * (u[i][j + 1][k] - u[i][j][k]) - KB
									* (u[i][j][k] - u[i][j - 1][k]))
							+ v
							* (KF * (u[i][j][k + 1] - u[i][j][k]) - KB
									* (u[i][j][k] - u[i][j][k - 1]))
							+ u[i][j][k] + f[i][j][k];
				}
		}
		for (i = 0; i <= nX + 1; ++i) {
			for (j = 0; j <= nY + 1; ++j)
				for (k = 0; k <= nZ + 1; ++k) {
					if (nu[i][j][k] == null)
						nu[i][j][k] = u[i][j][k];
				}
		}
	}

}
