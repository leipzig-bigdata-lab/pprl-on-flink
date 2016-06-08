package dbs.bigdata.flink.pprl;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import dbs.bigdata.flink.pprl.data.GeneratedDataLoader;

public class GeneratedDataLoaderTest {

	private GeneratedDataLoader loader;
	
	@Before
	public void initialize(){
		this.loader = new GeneratedDataLoader(null);
	}
	
	@Test
	public void test(){
		assertNotNull(loader);
	}
}
