package dbs.bigdata.flink.pprl;

import static org.junit.Assert.*;

import java.util.BitSet;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.junit.Before;
import org.junit.Test;

import dbs.bigdata.flink.pprl.functions.BlockReducer;
import dbs.bigdata.flink.pprl.utils.BloomFilterWithLshKeys;
import dbs.bigdata.flink.pprl.utils.CandidateBloomFilterPair;

/**
 * Class for testing the {@link BlockReducer}.
 * 
 * @author mfranke
 *
 */
public class BlockReducerTest {

	private final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

	private DataSet<Tuple3<Integer, BitSet, BloomFilterWithLshKeys>> data;
	private DataSet<Tuple2<Integer, CandidateBloomFilterPair>> newData;
	
	@Before
	public void init(){
		BitSet bitset = new BitSet(1000);
		bitset.set(12, 16);
		
		BloomFilterWithLshKeys bf1 = new BloomFilterWithLshKeys();
		bf1.setId("123");
		
		BloomFilterWithLshKeys bf2 = new BloomFilterWithLshKeys();
		bf1.setId("234");
		
		BloomFilterWithLshKeys bf3 = new BloomFilterWithLshKeys();
		bf1.setId("345");
		
		BloomFilterWithLshKeys bf4 = new BloomFilterWithLshKeys();
		bf1.setId("456");
		
		Tuple3<Integer, BitSet, BloomFilterWithLshKeys> tuple1
			= new Tuple3<Integer, BitSet, BloomFilterWithLshKeys>(0, bitset, bf1);
		
		Tuple3<Integer, BitSet, BloomFilterWithLshKeys> tuple2
			= new Tuple3<Integer, BitSet, BloomFilterWithLshKeys>(0, bitset, bf2);
		
		Tuple3<Integer, BitSet, BloomFilterWithLshKeys> tuple3
			= new Tuple3<Integer, BitSet, BloomFilterWithLshKeys>(0, new BitSet(), bf3);
		
		Tuple3<Integer, BitSet, BloomFilterWithLshKeys> tuple4
			= new Tuple3<Integer, BitSet, BloomFilterWithLshKeys>(0, bitset, bf4);
		
		Tuple3<Integer, BitSet, BloomFilterWithLshKeys> tuple5
			= new Tuple3<Integer, BitSet, BloomFilterWithLshKeys>(1, bitset, bf1);
	
		this.data = env.fromElements(tuple1, tuple2, tuple3, tuple4, tuple5);
	}
	
	@Test
	public void test() throws Exception {
		assertNotNull(this.data);
		assertEquals(5, this.data.count());
		
		this.newData = this.data.groupBy(0).reduceGroup(new BlockReducer());

		assertEquals(2, this.newData.count());
	}

}
