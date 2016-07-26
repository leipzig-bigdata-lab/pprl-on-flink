package dbs.bigdata.flink.pprl;

import static org.junit.Assert.*;

import java.util.BitSet;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Before;
import org.junit.Test;

import dbs.bigdata.flink.pprl.functions.DuplicateCandidateRemover;
import dbs.bigdata.flink.pprl.utils.BloomFilterWithLshKeys;
import dbs.bigdata.flink.pprl.utils.CandidateBloomFilterPair;

/**
 * Class for testing the {@link DuplicateCandidateRemoverTest} implementation.
 * 
 * @author mfranke
 *
 */
public class DuplicateCandidateRemoverTest {

	private final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
	private final int valueRange = 1000;
	private DataSet<Tuple2<Integer, CandidateBloomFilterPair>> data;
	
	@Before
	public void init(){
		
		CandidateBloomFilterPair pair1 = new CandidateBloomFilterPair();
		BloomFilterWithLshKeys bf1Pair1 = new BloomFilterWithLshKeys();
		BloomFilterWithLshKeys bf2Pair1 = new BloomFilterWithLshKeys();
		
		BitSet bitset1Pair1 = new BitSet(this.valueRange);
		bitset1Pair1.set(0, 100);
		
		BitSet bitset2Pair1 = new BitSet(this.valueRange);
		bitset2Pair1.set(100, 200);		
		
		BitSet[] keys = new BitSet[2];
		keys[0] = bitset1Pair1;
		keys[1] = bitset2Pair1;
		
		bf1Pair1.setLshKeys(keys);
		bf2Pair1.setLshKeys(keys);

		pair1.setCandidateOne(bf1Pair1);
		pair1.setCandidateTwo(bf2Pair1);
		
		Tuple2<Integer, CandidateBloomFilterPair> tuple1 =
				new Tuple2<Integer, CandidateBloomFilterPair>(0, pair1);
		
		Tuple2<Integer, CandidateBloomFilterPair> tuple2 =
				new Tuple2<Integer, CandidateBloomFilterPair>(1, pair1);
	
		this.data = env.fromElements(tuple1, tuple2);
	}
	
	@Test
	public void test() throws Exception {
		assertNotNull(this.data);
		assertEquals(2, this.data.count());
		
		DataSet<Tuple2<Integer, CandidateBloomFilterPair>> newData = 
				this.data.filter(new DuplicateCandidateRemover());
		
		assertNotNull(newData);
		assertEquals(1, newData.count());
	}

}