package dbs.bigdata.flink.pprl.functions;

import java.util.ArrayList;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;

import dbs.bigdata.flink.pprl.utils.BloomFilter;

public class BlockReducer implements GroupReduceFunction<Tuple3<String, BloomFilter, Integer>, 
	Tuple4<String, String, BloomFilter, BloomFilter>> {

	
	private static final long serialVersionUID = -788582704739580670L;

	@Override
	public void reduce(Iterable<Tuple3<String, BloomFilter, Integer>> values, 
			Collector<Tuple4<String, String, BloomFilter, BloomFilter>> out) throws Exception {
		
		ArrayList<Tuple3<String, BloomFilter, Integer>> valueList = this.getValuesInList(values);
		for (Tuple3<String, BloomFilter, Integer> value : valueList){
			for (Tuple3<String, BloomFilter, Integer> otherValue : valueList){
				if (!value.f0.equals(otherValue.f0)){
					out.collect(
						new Tuple4<String, String, BloomFilter, BloomFilter>(
								value.f0, otherValue.f0, value.f1,  otherValue.f1
						)
					);
				}
			}
		}
	}
	
	private ArrayList<Tuple3<String, BloomFilter, Integer>> getValuesInList(
			Iterable<Tuple3<String, BloomFilter, Integer>> values){
		
		ArrayList<Tuple3<String, BloomFilter, Integer>> result = 
			new ArrayList<Tuple3<String, BloomFilter, Integer>>();
		
		for (Tuple3<String, BloomFilter, Integer> value : values) {
			result.add(value);
		}
		
		return result;
	}

}
