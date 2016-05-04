package org.apache.flink.quickstart;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.runtime.operators.util.BloomFilter;
import org.apache.flink.util.Collector;

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over some sample data
 *
 * <p>
 * This example shows how to:
 * <ul>
 * <li>write a simple Flink program.
 * <li>use Tuple data types.
 * <li>write and use user-defined functions.
 * </ul>
 *
 */
public class WordCount {

	//
	//	Program
	//

	public static void main(String[] args) throws Exception {

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		/*
		// get input data
		DataSet<String> text = env.fromElements(
				"To be, or not to be,--that is the question:--",
				"Whether 'tis nobler in the mind to suffer",
				"The slings and arrows of outrageous fortune",
				"Or to take arms against a sea of troubles,"
				);
		
		
		DataSet<Tuple2<String, Integer>> counts =
				// split up the lines in pairs (2-tuples) containing: (word,1)
				text.flatMap(new LineSplitter())
				// group by the tuple field "0" and sum up tuple field "1"
				.groupBy(0)
				.sum(1);

		// execute and print result
		counts.print();
		*/
		
		// read all fields
		DataSet<Tuple4<String, String, String, String>> persons =
		  env.readCsvFile("/home/martin/Downloads/persons")
		    .lineDelimiter("##//##")
		    .fieldDelimiter("#|#")
		    .types(String.class, String.class, String.class, String.class);

		
		ArrayList<String> names = new ArrayList<String>();
		
		List<Tuple4<String, String, String, String>> personList = persons.collect();
		if (!personList.isEmpty()){
			for (int i = 0; i < personList.size(); i++){
				Tuple4<String, String, String, String> tupel = personList.get(i);
				names.add(tupel.f0 + " " + tupel.f1);
			}
		}
		
		ArrayList<String> tokens = getTokensFromNames(names);
		
		System.out.println(tokens.toString());
		
		BloomFilter bloomFilter = new BloomFilter(100, 32);
		
		bloomFilter.addHash(tokens.get(0).hashCode());
		
		
		System.out.println(bloomFilter.toString());

	}

	public static ArrayList<String> getTokensFromNames(ArrayList<String> names){
		ArrayList<String> tokens = new ArrayList<String>();
		
		for (String s : names){
			System.out.println(s);
			String token = "";
			int size = 3;
			char[] chars = s.toCharArray();
			
			for (int i = 0; i <= chars.length - size; i++){
				for (int j = i; j < i + size; j++){
					token = token + chars[j];
				}
				tokens.add(token);
				token = "";
			}
		}
		
		return tokens;
	}
	
	//
	// 	User Functions
	//

	/**
	 * Implements the string tokenizer that splits sentences into words as a user-defined
	 * FlatMapFunction. The function takes a line (String) and splits it into
	 * multiple pairs in the form of "(word,1)" (Tuple2<String, Integer>).
	 */
	public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<String, Integer>(token, 1));
				}
			}
		}
	}
}
