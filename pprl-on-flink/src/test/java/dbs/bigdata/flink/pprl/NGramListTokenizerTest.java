package dbs.bigdata.flink.pprl;

import static org.junit.Assert.*;

import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Before;
import org.junit.Test;

import dbs.bigdata.flink.pprl.data.Person;
import dbs.bigdata.flink.pprl.functions.NGramListTokenizer;

public class NGramListTokenizerTest {
	
	private final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
	private final String personId = "1234";
	
	private DataSet<Person> personData;
	private int ngram;
	private int tokenCount;

	
	@Before
	public void init(){
		Person person = new Person();
		person.setId(this.personId);
		person.setFirstName("Hans");
		person.setLastName("Müller");
		person.setAddressPartOne("Sesamestreet123");
		person.setCity("Wonderland");
		person.setAge("23");
		
		this.personData = env.fromElements(person);
		this.ngram = 2;
		
		String[] quids = person.getAttributeValues();
		
		for (String quid : quids){
			int plus = quid.length();
			if (plus >= 2){
				this.tokenCount = this.tokenCount + plus - 1;	
			}
		}
		
	}
	
	
	@Test
	public void test() throws Exception {
		assertNotNull(this.personData);
		assertEquals(1, this.personData.count());
		
		DataSet<Tuple2<String, List<String>>> newData = this.personData.flatMap(new NGramListTokenizer(this.ngram, false));
		
		assertNotNull(newData);
		
		assertEquals(1, newData.count());
		
		List<Tuple2<String, List<String>>> newDataInList = newData.collect();
		
		Tuple2<String, List<String>> tuple = newDataInList.get(0);
		
		assertEquals(this.personId, tuple.f0);
		
		assertEquals(this.tokenCount, tuple.f1.size());
		
		assertTrue(tuple.f1.contains("ll"));
	}
}