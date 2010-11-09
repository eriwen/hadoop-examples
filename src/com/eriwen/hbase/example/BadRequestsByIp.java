package com.eriwen.hbase.example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BadRequestsByIp extends Configured implements Tool {

	public static class MyMapper extends Mapper<Object, Text, ImmutableBytesWritable, Put> {
		private static final byte[] COLUMN_FAMILY_BYTES = Bytes.toBytes("mycolumnfamily");
		
		/**
		 * Called before processing each input split. In other words,
		 * before each file or 64MB of a large file.
		 * Make sure we have an HBase table to load into.
		 * 
		 * @param context - Map Context object
		 */
		@Override
		public void setup(final Context context) throws IOException {
			String tableName = context.getConfiguration().get("tableName");
			//Check for existence
			HBaseAdmin admin = new HBaseAdmin(HBaseConfiguration.create());
			if (admin.tableExists(tableName)) {
				//Create table if it doesn't
				HTableDescriptor table = new HTableDescriptor(tableName);
				HColumnDescriptor family = new HColumnDescriptor(COLUMN_FAMILY_BYTES);
				table.addFamily(family);
				admin.createTable(table);
			}
		}

		/**
		 * INPUT: Text "IP\tResource URL\tNumber of Bad Requests"
		 * OUTPUT: Key: byte[] for IP, Value: HBase Put object with each URL/Req # pair
		 *
		 * @param key - Unique Identifier for map (ignored)
		 * @param value - MapReduce Text object containing a String
		 * @param context - Map Context for reporting and configuration
		 */
		@Override
		public void map(final Object key, final Text value, final Context context)
		  	throws IOException, InterruptedException {
			
			String[] values = StringUtils.split(value.toString(), '\\', '\t');
			// Key is the IP
			if (values.length != 3) {
				// Count bad data lines
				context.getCounter("HBase Requests By IP Loader", "Bad Data").increment(1L);
				return;
			}
			byte[] rowKey = Bytes.toBytes(values[0]);
			Put put = new Put(rowKey);
			put.add(COLUMN_FAMILY_BYTES, Bytes.toBytes(values[1]), Bytes.toBytes(values[2]));
			context.write(new ImmutableBytesWritable(rowKey), put);
		}
	}
	
	/**
	 * Set MapReduce job parameters, classes, and other configurations.
	 */
	public int run(String[] args) throws Exception {
		Job job = new Job();
		job.setJobName("BadRequestsByIp");
		job.setJarByClass(BadRequestsByIp.class);
		job.setMapperClass(MyMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);
		
		HFileOutputFormat.configureIncrementalLoad(job, new HTable(args[2]));
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

 	  	boolean success = job.waitForCompletion(true);
	  	return success ? 0 : 1;
	}

	public static void main(final String[] args) throws Exception {
		int exitCode = ToolRunner.run(HBaseConfiguration.create(), new BadRequestsByIp(), args);
		System.exit(exitCode);
	}
}