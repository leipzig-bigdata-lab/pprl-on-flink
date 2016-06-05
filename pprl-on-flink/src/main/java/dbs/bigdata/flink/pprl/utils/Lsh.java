package dbs.bigdata.flink.pprl.utils;

import java.util.BitSet;

/**
 * Class, which implements the LSH functionality and the blocking.
 * @author mfranke
 */
public class Lsh {
	
	private int hashFunctions;
	private BloomFilter bloomFilter;
	private int blockCount;
	private int partitionSize;
	
	/**
	 * Creates a new Lsh object.
	 * @param hashFunctions
	 * 		-> the number of hash functions to use.
	 * 
	 * @param bloomFilter
	 * 		-> the bloom filter to be blocked.
	 * 
	 * @param blockCount
	 * 		-> the number of blocks which can possibly be filled.
	 * 
	 * @param partitionSize
	 * 		-> the size of the partitions the bloom filter should sliced into.
	 * 
	 * @throws Exception
	 * 		-> throws an exception, if the partitionSize is not smaller then the size of the bloom filter. 
	 */
	public Lsh(int hashFunctions, BloomFilter bloomFilter, int blockCount, int partitionSize) throws Exception{
		if (partitionSize < bloomFilter.getSize()){
			this.hashFunctions = hashFunctions;	
			this.bloomFilter = bloomFilter;
			this.blockCount = blockCount;
			this.partitionSize = partitionSize;
		}
		else{
			throw new Exception();
		}
	}
	
	private int hash1(BitSet bits){
		return Math.abs(HashUtils.getMD5(bits));
	}
		
	private int hash2(BitSet bits){
		return Math.abs(HashUtils.getSHA(bits));
	}

	public int[] getBlocks() {
		int[] blocks = new int[this.partitionSize * this.hashFunctions];
		int pointer = 0;
		
		BitSet[] subset = this.getSubsetsFromBloomFilter();
		for (int i = 0; i < subset.length; i++){
			for (int j = 0; j < this.hashFunctions; j++){
				blocks[pointer++] = (Math.abs(this.hash1(subset[i]) + j * this.hash2(subset[i]))) % blockCount;
			}
		}
		
		
		return blocks;
	}
	
	private BitSet[] getSubsetsFromBloomFilter(){
		BitSet bitset = this.bloomFilter.getBitset();
		int bitsetSize = bitset.size();
		int start = 0;
		int subsetSize = bitsetSize / this.blockCount;
		
		BitSet[] subsets = new BitSet[this.partitionSize];
		
		for (int i = 0; i < subsets.length; i++){
			int end;
			if (i == this.hashFunctions - 1){
				end = bitsetSize;
			}
			else{
				end = start + subsetSize;
			}
			subsets[i] = bitset.get(start, end);
			start = end;
		}
		return subsets;
	}

}
