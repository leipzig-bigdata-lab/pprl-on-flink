package dbs.bigdata.flink.pprl;

import static org.junit.Assert.*;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.*;

import dbs.bigdata.flink.pprl.data.*;
import dbs.bigdata.flink.pprl.functions.NGramTokenizer;

public class NGramTokenizerTest {

	final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

	private DataSet<Person> personData;
	private int ngram;
	private int quidStringLength;
	
	@Before
	public void init(){
		Person person = new Person();
		person.setFirstName("Hans");
		person.setLastName("Müller");
		person.setAddressPartOne("Sesamestreet123");
		person.setCity("Wonderland");
		person.setAge("23");
		
		this.personData = env.fromElements(person);
		this.ngram = 2;
		this.quidStringLength = person.getConcatenatedAttributes().length();
	}
	
	@Test
	public void testNGrams() throws Exception{
		FlatMapOperator<Person, Tuple2<String, String>> tokens = personData.flatMap(new NGramTokenizer(this.ngram));
		int countOfTokens = this.quidStringLength - 1;
		assertEquals(countOfTokens,tokens.count());
	}
}
