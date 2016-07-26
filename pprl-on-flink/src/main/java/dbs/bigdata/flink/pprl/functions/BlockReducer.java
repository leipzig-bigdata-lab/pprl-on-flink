package dbs.bigdata.flink.pprl.functions;

import java.util.ArrayList;
import java.util.BitSet;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import dbs.bigdata.flink.pprl.utils.BloomFilterWithLshKeys;
import dbs.bigdata.flink.pprl.utils.CandidateBloomFilterPair;

/**
 * Class for transforming {@link BloomFilterWithLshKeys} with the same key value for a blocking key
 * into a {@link CandidateBloomFilterPair} object.
 * 
 * @author mfranke
 */
public class BlockReducer
		implements GroupReduceFunction<Tuple3<Integer, BitSet, BloomFilterWithLshKeys>, 
			Tuple2<Integer, CandidateBloomFilterPair>> {

	
	private static final long serialVersionUID = -788582704739580670L;

	/**
	 * Transformation of (KeyId, KeyValue, {@link BloomFilterWithLshKeys})
	 * into (KeyId, {@link CandidateBloomFilterPair} for {@link BloomFilterWithLshKeys}
	 * with the same keyValue for a blocking key.
	 */
	@Override
	public void reduce(Iterable<Tuple3<Integer, BitSet, BloomFilterWithLshKeys>> values,
			Collector<Tuple2<Integer, CandidateBloomFilterPair>> out) throws Exception {
		
		ArrayList<Tuple3<Integer, BitSet, BloomFilterWithLshKeys>> valueList = this.getValuesInList(values);
		
		Integer keyId = valueList.get(0).f0;
		
		for (int i = 0; i < valueList.size(); i++){
			for (int j = i + 1; j < valueList.size(); j++){
				
				Tuple3<Integer, BitSet, BloomFilterWithLshKeys> value = valueList.get(i);
				Tuple3<Integer, BitSet, BloomFilterWithLshKeys> otherValue = valueList.get(j);
				
				String id1 = value.f2.getId();
				String id2 = otherValue.f2.getId();
				
				if (!id1.equals(id2)){
					BitSet key1 = value.f1;
					BitSet key2 = otherValue.f1;
						
					CandidateBloomFilterPair candidatePair =
							new CandidateBloomFilterPair(
									value.f2,
									otherValue.f2
							);
					
					if (key1.equals(key2)){
						out.collect(new Tuple2<Integer, CandidateBloomFilterPair>(keyId, candidatePair));
					}	
				}
			}
		}
	}

	private ArrayList<Tuple3<Integer, BitSet, BloomFilterWithLshKeys>> getValuesInList(
			Iterable<Tuple3<Integer, BitSet, BloomFilterWithLshKeys>> values){
		
		ArrayList<Tuple3<Integer, BitSet, BloomFilterWithLshKeys>> result = 
			new ArrayList<Tuple3<Integer, BitSet, BloomFilterWithLshKeys>>();
		
		for (Tuple3<Integer, BitSet, BloomFilterWithLshKeys> value : values) {
			result.add(value);
		}
		
		return result;
	}

}