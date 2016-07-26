package dbs.bigdata.flink.pprl.utils;

import java.util.BitSet;

/**
 * Class that represents a bloom filter with an id and lsh blocking keys.
 * 
 * @author mfranke
 *
 */
public class BloomFilterWithLshKeys {
	
	private String id;
	private BloomFilter bloomFilter;
	private BitSet[] lshKeys;
	
	
	/**
	 * Creates a new {@link BloomFilterWithLshKeys} object.
	 */
	public BloomFilterWithLshKeys(){
		this("", null, null);
	}
	
	/**
	 * Creates a new {@link BloomFilterWithLshKeys} object.
	 * 
	 * @param id
	 * 		-> an id for the bloom filter.
	 * 
	 * @param bloomFilter
	 * 		-> a bloom filter.
	 * 
	 * @param lshKeys
	 * 		-> an array of lsh blocking keys in form of a {@link BitSet}.
	 */
	public BloomFilterWithLshKeys(String id, BloomFilter bloomFilter, BitSet[] lshKeys){
		this.id = id;
		this.bloomFilter = bloomFilter;
		this.lshKeys = lshKeys;
	}

	public BloomFilter getBloomFilter() {
		return bloomFilter;
	}

	public void setBloomFilter(BloomFilter bloomFilter) {
		this.bloomFilter = bloomFilter;
	}

	public BitSet[] getLshKeys() {
		return this.lshKeys;
	}
	
	public BitSet getLshKeyAtPosition(int i){
		return this.lshKeys[i];
	}

	public void setLshKeys(BitSet[] lshKeys) {
		this.lshKeys = lshKeys;
	}
	
	public void setId(String id){
		this.id = id;
	}
	
	public String getId(){
		return this.id;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("BloomFilterWithLshKeys [id=");
		builder.append(id);
		/*
		builder.append(", bloomFilter=[");
		builder.append(bloomFilter);
		builder.append("], lshKeys=");
		builder.append(Arrays.toString(lshKeys));
		*/
		builder.append("]");
		return builder.toString();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		BloomFilterWithLshKeys other = (BloomFilterWithLshKeys) obj;
		if (id == null) {
			if (other.id != null) {
				return false;
			}
		} 
		else if (!id.equals(other.id)) {
			return false;
		}
		return true;
	}	
}