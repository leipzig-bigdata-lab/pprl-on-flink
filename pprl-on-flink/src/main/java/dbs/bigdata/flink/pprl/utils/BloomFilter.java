package dbs.bigdata.flink.pprl.utils;

import java.util.BitSet;

/**
 * A Bloom filter is a data structure for checking set membership
 * It is a bit array of length l with all bits initially set to 0. 
 * k independent hash functions are defined, which produce a output between 0 and l − 1. 
 * To store a element in the Bloom filter, it is hash coded using the k hash functions. 
 * All bits at the position corresponding to each hash value is set to one.
 * 
 * @author mfranke
 *
 */
public class BloomFilter {

	private final int size;
	private final int hashFunctions;
	private BitSet bitset;
	
	/**
	 * Empty default constructor for flink.
	 */
	public BloomFilter(){
		this(0,0,new BitSet());
	};
	
	/**
	 * Creates a new Bloom filter.
	 * @param size
	 * 		-> size of the bloom filter.
	 * 
	 * @param hashFunctions
	 * 		-> number of hash functions.
	 */
	public BloomFilter(int size, int hashFunctions){
		this(size, hashFunctions, new BitSet(size));
	}
	
	private BloomFilter(int size, int hashFunctions, BitSet bitset){
		this.size = size;
		this.hashFunctions = hashFunctions;
		this.bitset = bitset;
	}
	
	/**
	 * Add a element to this bloom filter.
	 * @param element
	 * @return int[] with the positions set to one.
	 */
	public int[] addElement(String element){
		int[] positions = new int[this.hashFunctions];
		for (int hashNumber = 0; hashNumber < this.hashFunctions; hashNumber++){
			int position = this.hashElement(element, hashNumber);
			this.bitset.set(position);
			positions[hashNumber] = position;
		}
		return positions;
	}
	
	private int hashElement(String element, int hashNumber){
		return (Math.abs(this.hash1(element) + hashNumber * this.hash2(element))) % this.size;
	}
	
	private int hash1(String element){
		return Math.abs(HashUtils.getMD5(element));
	}
		
	private int hash2(String element){
		return Math.abs(HashUtils.getSHA(element));
	}
	
	
	/**
	 * Sets all bits to zero.
	 */
	public void clear(){
		this.bitset.clear();
	}
	
	/**
	 * 
	 * @return number of bits set to one.
	 */
	public int getCardinality(){
		return this.bitset.cardinality();
	}
	
	/**
	 * Merge this bloom filter with another one.
	 *
	 * @param other
	 * 		-> the other bloom filter.
	 * 
	 * @return true, if the merging was successfully, else false. 
	 */
	public boolean mergeWith(BloomFilter other){
		if (other != null && other.bitset != null && this.size == other.size){
			this.bitset.or(other.bitset);
			return true;
		}
		else{
			return false;
		}
	}
	
	/**
	 * Merge this bloom filter with anonther and return the result.
	 * This bloom filter is not changed.
	 * 
	 * @param other
	 * 		-> the other bloom filter.
	 * 
	 * @return the merged bloom filter result.
	 */
	public BloomFilter merge(BloomFilter other){
		if (other != null && other.bitset != null && this.size == other.size){
			BitSet bitset = (BitSet) this.bitset.clone();
			bitset.or(other.bitset);
			return new BloomFilter(this.size, this.hashFunctions, bitset);
		}
		else{
			return null;
		}
	}
	
	/**
	 * Performs a logical OR of this bloom filter with the bloom filter argument. 
	 * This bloom filter is modified so that a bit in it has the value true if 
	 * and only if it either already had the value true or the corresponding bit in 
	 * the bit set argument has the value true. 
	 * 
	 * @param other
	 * 		-> the bloom filter argument.
	 * 
	 * @return
	 * 		-> the resulting bloom filter after the logic OR operation.
	 */
	public BloomFilter or(BloomFilter other){
		return this.merge(other);
	}
	
	/**
	 * Performs a logical AND of this target bloom filter with the argument bloom filter.
	 * This bloom filter is modified so that each bit in it has the value true if and only 
	 * if it both initially had the value true and the corresponding bit in the bloom filter
	 * argument also had the value true.
	 * 
	 * @param other
	 * 		-> the bloom filter argument.
	 * 
	 * @return
	 * 		-> the resulting bloom filter after the logic AND operation.
	 */
	public BloomFilter and(BloomFilter other){
		if (other != null && other.bitset != null && this.size == other.size){
			BitSet bitset = (BitSet) this.bitset.clone();
			bitset.and(other.bitset);
			return new BloomFilter(this.size, this.hashFunctions, bitset);
		}
		else{
			return null;
		}
	}
	
	/**
	 * Performs a logical XOR of this target bloom filter with the argument bloom filter.
	 *  This bloom filter is modified so that a bit in it has the value true if and only if
	 *   one of the following statements holds: 
	 *   	- The bit initially has the value true, and the corresponding bit in the 
	 *        argument has the value false. 
	 *      - The bit initially has the value false, and the corresponding bit in the 
	 *        argument has the value true. 
	 * 
	 * @param other
	 * 		-> the bloom filter argument.
	 * 
	 * @return
	 * 		-> the resulting bloom filter after the logic XOR operation.
	 */		
	public BloomFilter xor(BloomFilter other){
		if (other != null && other.bitset != null && this.size == other.size){
			BitSet bitset = (BitSet) this.bitset.clone();
			bitset.xor(other.bitset);
			return new BloomFilter(this.size, this.hashFunctions, bitset);
		}
		else{
			return null;
		}
	}
	
	@Override
	public String toString(){
		return this.bitset.toString();
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
		BloomFilter other = (BloomFilter) obj;
		if (bitset == null) {
			if (other.bitset != null) {
				return false;
			}
		} 
		else if (!bitset.equals(other.bitset)) {
			return false;
		}
		if (size != other.size) {
			return false;
		}
		return true;
	}

	public BitSet getBitset() {
		return bitset;
	}

	public void setBitset(BitSet bitset) {
		this.bitset = bitset;
	}

	public int getSize() {
		return size;
	}

	public int getHashFunctions() {
		return hashFunctions;
	}

}