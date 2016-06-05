package dbs.bigdata.flink.pprl.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;

import dbs.bigdata.flink.pprl.utils.BloomFilter;
import dbs.bigdata.flink.pprl.utils.Lsh;

public class LshBlocker 
	implements FlatMapFunction<Tuple2<String, BloomFilter>, Tuple3<String, BloomFilter, Integer>> {

	
	private static final long serialVersionUID = 5273583064581285374L;

	private int hashFunctions;
	private int blockCount;
	private int partitionSize;
	
	public LshBlocker(int hashFunctions, int blockCount, int partitionSize){
		this.hashFunctions = hashFunctions;
		this.blockCount = blockCount;
		this.partitionSize = partitionSize;
	}
	
	@Override
	public void flatMap(Tuple2<String, BloomFilter> value, Collector<Tuple3<String, BloomFilter, Integer>> out)
			throws Exception {
			
		Lsh lsh = new Lsh(this.hashFunctions, value.f1, this.blockCount, this.partitionSize);
		int[] block = lsh.getBlocks();
		
		for (int i = 0; i < block.length; i++){
			out.collect(
				new Tuple3<String, BloomFilter, Integer>(
					value.f0,
					value.f1,
					block[i]
				)
			);
		}
	}

}
