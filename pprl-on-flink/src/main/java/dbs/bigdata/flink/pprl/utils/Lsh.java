package dbs.bigdata.flink.pprl.utils;

import java.util.BitSet;

public class Lsh {
	
	private int hashFunctions;
	private BloomFilter bloomFilter;
	private int blockCount;
	private int partitionSize;
	
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
