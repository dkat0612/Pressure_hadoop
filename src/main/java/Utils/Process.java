package Utils;

public class Process {
	private Integer rank;
	private Integer up;
	private Integer down;
	private Integer size;

	public Process(Integer aRank, Integer P) {
		this.rank = aRank;
		this.size = P;
		if (rank > 0) {
			up = rank - 1;
		} else
			up = null;
		if (rank < size- 1) {
			down = rank + 1;
		} else
			down = null;
	}

	public Integer getRank() {
		return rank;
	}

	public Integer getUp() {
		return up;
	}

	public Integer getDown() {
		return down;
	}

}
