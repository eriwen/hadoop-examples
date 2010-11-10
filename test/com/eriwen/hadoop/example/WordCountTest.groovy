package com.eriwen.hadoop.example

import org.apache.hadoop.mapreduce.*
import org.mrunit.mapreduce.*
import org.junit.*
import static org.junit.Assert.*

class WordCountTest {
	private Mapper mapper
	private Reducer reducer
	private MapDriver mapDriver
	private ReduceDriver reduceDriver
	
	@Before void setUp() {
		// Mock setup method so we don't have to deal with reading a file
		mapper = [setup:{c-> mapper.interestingWordsSet.add('world')}] as WordCount.WordCountMapper()
		reducer = new WordCount.WordCountReducer()
		mapDriver = new MapDriver(mapper)
		reduceDriver = new ReduceDriver(reducer)
	}
	
	@After void tearDown() {
		reduceDriver = null
		mapDriver = null
		reducer = null
		mapper = null
	}
	
	@Test void testMap() {
		mapDriver.withInput(new LongWritable(42), new Text('hello world goodbye world'))
		def result = mapDriver.run()
		assertEquals('world', result[0].first.toString())
		
		// Verify counters
		def counters = mapDriver.getCounters()
		assertEquals(2, counters.findCounter('WordCount', 'UNinteresting Words').getValue())
	}
	
	@Test void testReduce() {
		reduceDriver.withInput(new Text('world'), [new IntWritable(1), new IntWritable(1)] as List)
			.withOutput(new Text('world'), new IntWritable(2))
			.runTest()
	}
}