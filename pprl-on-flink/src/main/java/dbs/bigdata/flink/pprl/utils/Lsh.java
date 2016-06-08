package dbs.bigdata.flink.pprl.utils;

import java.util.BitSet;

/**
 * Class, which implements the LSH functionality and the blocking.
 * @author mfranke
 */
public class Lsh {
	
	@SuppressWarnings("unused")
	private int hashFunctions; // can be removed if not needed
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
	
	@SuppressWarnings("unused")
	private int hash1(BitSet bits){
		return Math.abs(HashUtils.getMD5(bits));
	}
		
	private int hash2(BitSet bits){
		return Math.abs(HashUtils.getSHA(bits));
	}

	public int[] getBlocks() {
		int[] blocks = new int[this.partitionSize];
		int blockPointer = 0;
		
		// divide the bloom filter in subsets of partitionSize
		BitSet[] subset = this.getSubsetsFromBloomFilter();
		
		// hash each subset into blockCount blocks
		// the hash value builds the block number and is independent for the subsets
		for (int i = 0; i < subset.length; i++){
				blocks[blockPointer++] = 
						(Math.abs(this.hash2(subset[i])) %  blockCount) + (blockCount * i);
		}
		
		return blocks;
	}
	
	private BitSet[] getSubsetsFromBloomFilter(){
		BitSet bitset = this.bloomFilter.getBitset();
		int bitsetSize = bitset.size();
		int start = 0;
		int subsetSize = bitsetSize / this.partitionSize;
		
		BitSet[] subsets = new BitSet[this.partitionSize];
		
		for (int i = 0; i < subsets.length; i++){
			int end;
			if (i == subsets.length - 1){
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
