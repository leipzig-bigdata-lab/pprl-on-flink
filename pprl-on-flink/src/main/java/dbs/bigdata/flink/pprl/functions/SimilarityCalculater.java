package dbs.bigdata.flink.pprl.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;

import dbs.bigdata.flink.pprl.utils.BloomFilter;

public class SimilarityCalculater implements 
	FlatMapFunction<Tuple4<String, String, BloomFilter, BloomFilter>, 
		Tuple2<String, String>> {

	private static final long serialVersionUID = -5284383227828157168L;
	private double threshold;
	
	public SimilarityCalculater(double threshold){
		this.threshold = threshold;
	}
	
	@Override
	public void flatMap(Tuple4<String, String, BloomFilter, BloomFilter> value, 
			Collector<Tuple2<String, String>> out) throws Exception {

		BloomFilter bf1 = value.f2;
		BloomFilter bf2 = value.f3;
		
		BloomFilter and = bf1.and(bf2);
		BloomFilter or = bf1.or(bf2);
				
		double similarityValue = ((double) and.getCardinality()) / or.getCardinality();
		
		if (similarityValue >= this.threshold){
			out.collect(new Tuple2<String, String>(value.f0, value.f1));
		}
	}

}
