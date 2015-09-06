package com.emc.solr;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class Read_Seq_file {
	
public static class SeqMapper extends Mapper<Text, BytesWritable, Text, BytesWritable> {

public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException  {
	      String filename = key.toString();
	      if(filename.equals("table.pdf"))
	    	  context.write(key,value);
	    
	    }
	}

public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
	
	Configuration conf = new Configuration();
    
    Job job = new Job(conf, "Word Counter");
	
    job.setJarByClass( Read_Seq_file.class );
    job.setMapperClass( SeqMapper.class );
    
    
    job.setMapOutputKeyClass( Text.class );
    job.setMapOutputValueClass( BytesWritable.class );
   
    job.setNumReduceTasks(0);
    
    
	job.setInputFormatClass(SequenceFileInputFormat.class);
	
	
	
    
	SequenceFileInputFormat.addInputPath(job,new Path("seqpdf/part-r-00000"));
    FileOutputFormat.setOutputPath(job,new Path("outputpdf/"));
    
    System.exit( job.waitForCompletion( true ) ? 0 : 1 );
}
}
