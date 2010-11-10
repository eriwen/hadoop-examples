package com.eriwen.hadoop.example;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class WordCount extends Configured implements Tool {
	public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
		final Set<String> interestingWordsSet = new HashSet<String>();
		//Construct the Text only once and reuse
		private final Text mapKeyText = new Text();
		private static final IntWritable ONE = new IntWritable(1);

		/**
		 * Called before processing each input split. In other words,
		 * before each file or 64MB of a large file.
		 * Load interesting words into a HashSet to check later.
		 * 
		 * @param context - Map Context object
		 */
		@Override
		public void setup(final Context context) throws IOException {
			//Load words from the DistributedCache
			File interestingWordsFile = new File("words.txt");
			BufferedReader reader;
			FileReader fr;
			try {
				fr = new FileReader(interestingWordsFile);
				reader = new BufferedReader(fr);
				String word;
				while ((word = reader.readLine()) != null) {
					interestingWordsSet.add(word);
				}
				fr.close();
				reader.close();
			} catch (IOException ioe) {
			  throw new RuntimeException("Could not populate word set", ioe);
			}
		}
		
		/**
		 * INPUT: Block of text
		 * OUTPUT: Key: Text Word, Value: 1
		 *
		 * @param key - Unique Identifier for map (ignored)
		 * @param value - MapReduce Text object containing a String
		 * @param context - Map Context for reporting and configuration
		 */
		@Override
		public void map(final Object key, final Text value, final Context context)
		  		throws IOException, InterruptedException {
			// Split String into words, naively
			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			while (tokenizer.hasMoreTokens()) {
				if (interestingWordsSet.contains(value)) {
					mapKeyText.set(tokenizer.nextToken());
					context.write(mapKeyText, ONE);
				} else {
					context.getCounter("WordCount", "UNinteresting Words").increment(1L);
				}
			}
		}
	}
	
	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private final IntWritable reduceValue = new IntWritable(0);
		
		/**
		 * Given a key (word) and a collection of IntWritables, just sum them up.
		 * 
		 * @param key - word as Text
		 * @param records - Count of a given word
		 * @param context - Reduce Context object
		 */
		@Override
		public void reduce(final Text key, final Iterable<IntWritable> records,
				final Context context) throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable record : records) {
				sum += record.get();
			}
			reduceValue.set(sum);
			context.write(key, reduceValue);
		}
	}
	
	/**
	 * Set MapReduce job parameters, classes, and other configurations.
	 */
	public int run(final String[] args) throws Exception {
		Job job = new Job();
		job.setJobName("WordCount");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(WordCountMapper.class);
		job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(WordCountReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

 	  	boolean success = job.waitForCompletion(true);
	  	return success ? 0 : 1;
	}

	public static void main(final String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new WordCount(), args);
		System.exit(exitCode);
	}
}