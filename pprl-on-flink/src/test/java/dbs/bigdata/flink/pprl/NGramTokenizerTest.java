package dbs.bigdata.flink.pprl;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.*;

import dbs.bigdata.flink.pprl.data.*;
import dbs.bigdata.flink.pprl.functions.NGramTokenizer;

/**
 * Class for testing the {@link NGramTokenizer} implementation, i.e. the
 * splitting of the person attributes into tokens with flink.
 * 
 * @author mfranke
 *
 */
public class NGramTokenizerTest {

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
	public void testNGrams() throws Exception{
		
		DataSet<Tuple2<String, String>> tokens = 
				personData.flatMap(new NGramTokenizer(this.ngram, false));
		
		assertNotNull(tokens);
				
		assertEquals(tokenCount, tokens.count());
		
		List<Tuple2<String, String>> tokenList = tokens.collect();
		
		assertFalse(tokenList.isEmpty());
		
		List<String> allTokens = new ArrayList<String>();
		for (Tuple2<String, String> tuple : tokenList){
			assertEquals(this.personId, tuple.f0);
			allTokens.add(tuple.f1);
		}
		
		assertTrue(allTokens.contains("ha"));
		assertTrue(allTokens.contains("an"));
		assertTrue(allTokens.contains("nd"));
	}
}