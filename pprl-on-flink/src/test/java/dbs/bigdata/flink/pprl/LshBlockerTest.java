package dbs.bigdata.flink.pprl;

import static org.junit.Assert.*;

import java.util.BitSet;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.junit.Before;
import org.junit.Test;

import dbs.bigdata.flink.pprl.functions.LshBlocker;
import dbs.bigdata.flink.pprl.utils.BloomFilter;
import dbs.bigdata.flink.pprl.utils.BloomFilterWithLshKeys;
import dbs.bigdata.flink.pprl.utils.HashFamilyGroup;
import dbs.bigdata.flink.pprl.utils.IndexHash;

/**
 * Class for testing the {@link LshBlocker}, i.e. the lsh functionality in flink.
 * 
 * @author mfranke
 *
 */
public class LshBlockerTest {

	private final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
	private final int bfSize = 1000;
	private final int bfHashes = 4;
	private final String id = "ID1";
	private final int numberOfHashFamilies = 10;
	private final int numberOfHashesPerFamily = 50;
	
	private DataSet<Tuple2<String, BloomFilter>> data;
	
	
	@Before
	public void init(){		
		BloomFilter bf = new BloomFilter(this.bfSize, this.bfHashes);
		bf.addElement("foo");
		bf.addElement("bar");
		
		Tuple2<String, BloomFilter> tuple1 = new Tuple2<String, BloomFilter>(this.id, bf);
		
		this.data = env.fromElements(tuple1);
	}
	
	@Test
	public void test() throws Exception {
		assertEquals(1, this.data.count());
		
		HashFamilyGroup<IndexHash, Boolean> hashFamilyGroup = 
				HashFamilyGroup.generateRandomIndexHashFamilyGroup(
						this.numberOfHashFamilies, 
						this.numberOfHashesPerFamily, 
						this.bfSize
				);
				
		DataSet<Tuple3<Integer, BitSet, BloomFilterWithLshKeys>> newData = 
				this.data.flatMap(new LshBlocker(hashFamilyGroup));
		
		assertEquals(this.numberOfHashFamilies, newData.count());
		
		List<Tuple3<Integer, BitSet, BloomFilterWithLshKeys>> elementList = newData.collect();
		
		for (int i = 0; i < elementList.size(); i++){
			Tuple3<Integer, BitSet, BloomFilterWithLshKeys> element = elementList.get(i);
			
			assertTrue(element.f0 >= 0 && element.f0 < this.numberOfHashFamilies);
			
			assertTrue(this.numberOfHashesPerFamily <= element.f1.size());
			
			assertEquals(this.id, element.f2.getId());
			
			assertEquals(this.numberOfHashFamilies, element.f2.getLshKeys().length);
		}
	}

}