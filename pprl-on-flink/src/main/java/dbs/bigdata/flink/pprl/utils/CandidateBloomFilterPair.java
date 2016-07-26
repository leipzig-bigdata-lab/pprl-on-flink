package dbs.bigdata.flink.pprl.utils;

/**
 * Class that represents a candidate bloom filter pair of two {@link BloomFilterWithLshKeys} objects.
 * 
 * @author mfranke
 *
 */
public class CandidateBloomFilterPair {

	private BloomFilterWithLshKeys candidateOne;
	private BloomFilterWithLshKeys candidateTwo;
	
	/**
	 * Creates a new {@link CandidateBloomFilterPair} object.
	 */
	public CandidateBloomFilterPair(){
		this(null, null);
	}
	
	/**
	 * Creates a new {@link CandidateBloomFilterPair} object.
	 * 
	 * @param candidateOne
	 * 		-> a {@link BloomFilterWithLshKeys} object.
	 *
	 * @param candidateTwo
	 * 		-> an other {@link BloomFilterWithLshKeys} object.
	 */
	public CandidateBloomFilterPair(BloomFilterWithLshKeys candidateOne, BloomFilterWithLshKeys candidateTwo){
		this.candidateOne = candidateOne;
		this.candidateTwo = candidateTwo;
	}

	public BloomFilterWithLshKeys getCandidateOne() {
		return candidateOne;
	}

	public void setCandidateOne(BloomFilterWithLshKeys candidateOne) {
		this.candidateOne = candidateOne;
	}

	public BloomFilterWithLshKeys getCandidateTwo() {
		return candidateTwo;
	}

	public void setCandidateTwo(BloomFilterWithLshKeys candidateTwo) {
		this.candidateTwo = candidateTwo;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		
		if (obj == null) {
			return false;
		}
		
		if (getClass() != obj.getClass()) {
			return false;
		}
		
		CandidateBloomFilterPair other = (CandidateBloomFilterPair) obj;
		
		if (candidateOne == null) {
			if (other.candidateOne != null | other.candidateTwo != null) {
				return false;
			}
		}
		
		if (candidateTwo == null) {
			if (other.candidateTwo != null | other.candidateOne != null) {
				return false;
			}
		}
		
		if (candidateOne == null && candidateTwo == null){
			if (other.candidateOne != null && other.candidateTwo != null){
				return false;
			}
		}
		
		if (candidateOne.equals(other.candidateOne) && candidateTwo.equals(other.candidateTwo) ) {
			return true;
		}
		
		if (candidateOne.equals(other.candidateTwo) && candidateTwo.equals(other.candidateOne)){
			return true;
		}
		
		return false;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("CandidateBloomFilterPair [candidateOne=");
		builder.append(candidateOne);
		builder.append(", candidateTwo=");
		builder.append(candidateTwo);
		builder.append("]");
		return builder.toString();
	}	
}