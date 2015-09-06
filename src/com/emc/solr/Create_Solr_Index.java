package com.emc.solr;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.StringTokenizer;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.ContentHandlerDecorator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;



public class Create_Solr_Index {
	
	public static void main(String[] args) throws IOException,
    InterruptedException, ClassNotFoundException {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser( conf, args ).getRemainingArgs();
		Job job = new Job(conf);
		job.setJobName("Build Index");
		job.setJarByClass(Create_Solr_Index.class);
		
		job.setMapperClass(IndexMapper.class);
		job.setReducerClass(Reducer.class);
		
		job.setNumReduceTasks(1);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		 
		job.setInputFormatClass(WholeFileInputFormat.class);
		WholeFileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
	//	MultipleInputs.addInputPath(job, new Path("har:///user/notroot/outputpdf/pdf.har"),WholeFileInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path("tempdir"));

		job.waitForCompletion(true);
	}
	
	
	public static class IndexMapper extends Mapper <NullWritable, BytesWritable, NullWritable, NullWritable> {
	  HttpSolrServer server = null;
	  Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
	  
	  @Override
	  protected void setup(Context context) throws IOException,InterruptedException  {
	    String url = "http://localhost:8983/solr";
	    server = new HttpSolrServer(url);
	  }

	  @Override
	  public void map(NullWritable key, BytesWritable val, Context context) throws IOException {

		  Parser parser = new AutoDetectParser();
          String content="";
          SolrInputDocument document = new SolrInputDocument();
          ContentHandler handler = new BodyContentHandler();
          Metadata metadata = new Metadata();
          ByteArrayInputStream in = new ByteArrayInputStream(val.getBytes());
          System.out.println("Parser starts");
          String filePath = ((FileSplit) context.getInputSplit()).getPath().toString();
          document.addField("id", filePath);
          try {
			parser.parse(in, handler, metadata, new ParseContext());
          } catch (SAXException e1) {
			e1.printStackTrace();
          } catch (TikaException e1) {
			e1.printStackTrace();
          }
          ContentHandlerDecorator cd = new ContentHandlerDecorator(handler);
          content = cd.toString();
          StringTokenizer contentTokens = new StringTokenizer(content);
          
          while(contentTokens.hasMoreTokens()) {
        	  document.addField("content",contentTokens.nextToken());
	            }
	      try {
	    	docs.add(document);
	        server.add(docs);
	      } catch (SolrServerException e) {
	        e.printStackTrace();
	      } catch (IOException e) {
	        e.printStackTrace();
	      }
	    }
	  @Override
	  protected void cleanup(Context context) throws IOException {
	  try {

		  server.commit();
		  UpdateRequest req = new UpdateRequest();
          req.setAction( UpdateRequest.ACTION.COMMIT, false, false );
          req.add( docs );
          UpdateResponse rsp = req.process( server );
	    } catch (SolrServerException e) {
	      e.printStackTrace();
	    }
	  }
	}
	
}
