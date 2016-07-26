package dbs.bigdata.flink.pprl.functions;

import java.util.BitSet;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;
import dbs.bigdata.flink.pprl.utils.BloomFilter;
import dbs.bigdata.flink.pprl.utils.BloomFilterWithLshKeys;
import dbs.bigdata.flink.pprl.utils.HashFamilyGroup;
import dbs.bigdata.flink.pprl.utils.IndexHash;
import dbs.bigdata.flink.pprl.utils.Lsh;

/**
 * Class for building blocks from the bloom filter.
 * Therefore the LshBlocker hashes pieces of the bloom filter
 * and generates a blocking key. 
 * 
 * @author mfranke
 *
 */
public class LshBlocker 
	implements FlatMapFunction<Tuple2<String, BloomFilter>, Tuple3<Integer, BitSet, BloomFilterWithLshKeys>> {

	
	private static final long serialVersionUID = 5273583064581285374L;

	private HashFamilyGroup<IndexHash, Boolean> hashFamilyGroup;
	
	public LshBlocker(HashFamilyGroup<IndexHash, Boolean> hashFamilyGroup){
		this.hashFamilyGroup = hashFamilyGroup;
	}
	
	/**
	 * Transformation of (Id, {@link BloomFilter}) tuples
	 * into (KeyId, KeyValue, {@link BloomFilterWithLshKeys}).
	 * This transformation executes the first blocking step.
	 */
	@Override
	public void flatMap(Tuple2<String, BloomFilter> value, 
			Collector<Tuple3<Integer, BitSet, BloomFilterWithLshKeys>> out) throws Exception {
			
		Lsh<IndexHash> lsh = new Lsh<IndexHash>(value.f1, this.hashFamilyGroup);
		BitSet[] lshKeys = lsh.calculateKeys();
		
		BloomFilterWithLshKeys bfWithKeys = new BloomFilterWithLshKeys(value.f0, value.f1, lshKeys);
		
		for (int keyId = 0; keyId < lshKeys.length; keyId++){
			out.collect(
				new Tuple3<Integer, BitSet, BloomFilterWithLshKeys>(
					keyId,
					lshKeys[keyId],
					bfWithKeys
				)
			);
		}
	}

}
