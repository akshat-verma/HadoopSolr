package com.emc.solr;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
public class Convert_to_Sequence {
	
	public static class SeqMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {

	public void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException  {
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String filename = fileSplit.getPath().getName();
		     context.write(new Text(filename), value);
			    }
		}
	
 
	public static void main(String[] args) throws IOException,
    InterruptedException, ClassNotFoundException {

		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJobName("Convert Text");
		job.setJarByClass(Convert_to_Sequence.class);
		
		job.setMapperClass(SeqMapper.class);
		job.setReducerClass(Reducer.class);
		
		job.setNumReduceTasks(1);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setInputFormatClass(WholeFileInputFormat.class);
		
		WholeFileInputFormat.addInputPath(job, new Path("pdf/"));
		SequenceFileOutputFormat.setOutputPath(job, new Path("seqpdf/"));
		
		job.waitForCompletion(true);
	}
}
