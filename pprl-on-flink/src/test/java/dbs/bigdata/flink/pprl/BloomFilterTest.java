package dbs.bigdata.flink.pprl;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class BloomFilterTest {

	private BloomFilter bloomFilter;
	
	@Before
	public void initializeBloomFilter(){
		int size = 100;
		int hashes = 4;
		this.bloomFilter = new BloomFilter(size, hashes);
	}
	
	@Test
	public void testAddElement(){
		assertTrue(this.bloomFilter.getBitSet().isEmpty());
		int[] positions = this.bloomFilter.addElement("test");
		assertFalse(this.bloomFilter.getBitSet().isEmpty());
		assertEquals(positions.length, this.bloomFilter.getNumberOfHashFunctions());
	}
	
	@Test
	public void testEquals(){
		assertFalse(this.bloomFilter.equals(new BloomFilter(99,4)));
		BloomFilter otherBloomFilter = new BloomFilter(100,4);
		this.bloomFilter.addElement("foo");
		otherBloomFilter.addElement("bar");
		assertFalse(this.bloomFilter.equals(otherBloomFilter));
		otherBloomFilter.clear();
		otherBloomFilter.addElement("foo");
		assertTrue(this.bloomFilter.equals(otherBloomFilter));
	}
	
	@Test
	public void testMerge(){
		
	}
	
	
}
