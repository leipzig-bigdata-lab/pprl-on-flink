package dbs.bigdata.flink.pprl.functions;

import java.util.BitSet;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import dbs.bigdata.flink.pprl.utils.BloomFilterWithLshKeys;
import dbs.bigdata.flink.pprl.utils.CandidateBloomFilterPair;

/**
 * Class for removing duplicate candidate pairs for different blocking keys.
 * More precisely if the blocking key values of two bloom filter for a blocking key smaller than
 * the "current" are equal then the candidate pair is removed. This is because the candidate
 * pair is then compared for an other key and so the comparison here would be unnecessary. 
 * 
 * @author mfranke
 *
 */
public class DuplicateCandidateRemover implements FilterFunction<Tuple2<Integer, CandidateBloomFilterPair>> {

	private static final long serialVersionUID = 47428697482093145L;

	/**
	 * Filter out duplicate candidate bloom filter pairs (pairs that are compared already in an other
	 * block (an other key)).
	 */
	@Override
	public boolean filter(Tuple2<Integer, CandidateBloomFilterPair> value) throws Exception {
		int keyId = value.f0.intValue();
		
		if (keyId == 0){
			return true;	
		}
		else{
			BloomFilterWithLshKeys bf1 = value.f1.getCandidateOne();
			BloomFilterWithLshKeys bf2 = value.f1.getCandidateTwo();
			
			while (keyId > 0){	
				keyId--;
				
				BitSet keyValue1 = bf1.getLshKeyAtPosition(keyId);
				BitSet keyValue2 = bf2.getLshKeyAtPosition(keyId);
				
				if (keyValue1.equals(keyValue2)){
					return false;
				}
			}
			return true;	
		}
	}

}