package com.bigdata.acadgild;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvalidRecords {

	public static void main(String[] args) throws Exception {

		Configuration con = new Configuration();
		Job job = new Job(con);
		job.setJobName("finding NA records");
		job.setJarByClass(InvalidRecords.class);
		
		job.setMapperClass(InvalidRecordFinder.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}

	private static class InvalidRecordFinder extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text,Text,Text> {
		private final static String NA="NA";
		private final static Text empty = new Text("");
		public void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context ) throws IOException, InterruptedException
		{
			String[] values = value.toString().split("\\|");
			if ( NA.equals(values[0]) || NA.equals(values[1]) )
			{
				context.write(value, empty);
			}
		}
	}

}