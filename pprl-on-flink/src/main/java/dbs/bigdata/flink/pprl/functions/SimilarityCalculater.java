package dbs.bigdata.flink.pprl.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;

import dbs.bigdata.flink.pprl.utils.BloomFilter;
import dbs.bigdata.flink.pprl.utils.BloomFilterWithLshKeys;
import dbs.bigdata.flink.pprl.utils.CandidateBloomFilterPair;

/**
 * Class for calculating similarity of a {@link CandidateBloomFilterPair}.
 * 
 * @author mfranke
 *
 */
public class SimilarityCalculater implements 
	FlatMapFunction<Tuple2<Integer,CandidateBloomFilterPair>, 
		Tuple2<String, String>> {

	private static final long serialVersionUID = -5284383227828157168L;
	private double threshold;
	
	public SimilarityCalculater(double threshold){
		this.threshold = threshold;
	}
	
	/**
	 * Transformation of {@link CandidateBloomFilterPair}s into a set of (Id1, Id2) tuples.
	 * These tuples are the ids of the person that matched.
	 */
	@Override
	public void flatMap(Tuple2<Integer,CandidateBloomFilterPair> value, 
			Collector<Tuple2<String, String>> out) throws Exception {

		BloomFilterWithLshKeys bfCandidateOne = value.f1.getCandidateOne();
		BloomFilterWithLshKeys bfCandidateTwo = value.f1.getCandidateTwo();
		
		BloomFilter bf1 = bfCandidateOne.getBloomFilter();
		BloomFilter bf2 = bfCandidateTwo.getBloomFilter();
		
		BloomFilter and = bf1.and(bf2);
		BloomFilter or = bf1.or(bf2);
				
		double similarityValue = ((double) and.getCardinality()) / or.getCardinality();
		
		if (similarityValue >= this.threshold){
			out.collect(new Tuple2<String, String>(bfCandidateOne.getId(), bfCandidateTwo.getId()));
		}
	}
}