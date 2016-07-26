package dbs.bigdata.flink.pprl;

import static org.junit.Assert.*;

import java.util.BitSet;

import org.junit.Before;
import org.junit.Test;

import dbs.bigdata.flink.pprl.utils.BloomFilter;
import dbs.bigdata.flink.pprl.utils.HashFamilyGroup;
import dbs.bigdata.flink.pprl.utils.IndexHash;
import dbs.bigdata.flink.pprl.utils.Lsh;

/**
 * Class for testing the {@link Lsh} implementation.
 * 
 * @author mfranke
 *
 */
public class LshTest {

	private Lsh<IndexHash> lsh;
	private BloomFilter bf;
	private int valueRange = 1000;
	private int hashFunctionsBf = 8;
	private int numberOfHashFamilies = 10;
	private int numberOfHashesPerFamily = 50;
	
	@Before
	public void init(){
		this.bf = new BloomFilter(this.valueRange, this.hashFunctionsBf);
		this.bf.addElement("foo");
		this.bf.addElement("bar");
		this.bf.addElement("oof");
		this.bf.addElement("rab");
		
		HashFamilyGroup<IndexHash, Boolean> hashFamilyGroup = HashFamilyGroup.generateRandomIndexHashFamilyGroup(
				this.numberOfHashFamilies, 
				this.numberOfHashesPerFamily,
				this.valueRange
		);
		
		this.lsh = new Lsh<IndexHash>(this.bf, hashFamilyGroup);
	}
	
	@Test
	public void importantFact(){
		BitSet bitset = new BitSet(100);
		assertNotEquals(100, bitset.size());
	}
	
	@Test
	public void test() {
		assertNotNull(this.lsh);
		
		BitSet[] blockingKeys = this.lsh.calculateKeys();
		
		assertNotNull(blockingKeys);
		assertEquals(this.numberOfHashFamilies, blockingKeys.length);
		
		for (int i = 0; i < blockingKeys.length; i++){
			assertTrue(this.numberOfHashesPerFamily <= blockingKeys[i].size());
		}
	}

}
