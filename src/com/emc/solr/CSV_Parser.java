package com.emc.solr;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;


public class CSV_Parser {
	
	public static void main(String[] args){
		
				String url = "http://localhost:8983/solr";
				HttpSolrServer server = new HttpSolrServer(url);
		        InputStream is = null;
		        String path = "bank-16.csv";
		        String content = "";

		        try {
		            Scanner scan = new Scanner(new FileInputStream(path));
		            while(scan.hasNext()){
		            	content = content+scan.nextLine().toString();
		            }
		            StringTokenizer st = new StringTokenizer(content);
		            Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		            //server.deleteById("");
		            //server.commit();
		            SolrInputDocument document = new SolrInputDocument();
		            document.addField("id","har://hdfs-localhost:8020/user/notroot/outputpdf/pdf.har/"+path);
		            System.out.println(document);
		            
		            while(st.hasMoreTokens()) {
		            	document.addField("content",st.nextToken());
		            }
		            
		            //System.out.println("document:\t" + document);
		            System.out.println("the content is \t" + content);
		            System.out.println("Parser ends");
		            System.out.println("document:\t" + document);
		           
		            docs.add(document);
		            server.add(docs);
		            server.commit();
		            
		            UpdateRequest req = new UpdateRequest();
		            req.setAction( UpdateRequest.ACTION.COMMIT, false, false );
		            req.add( docs );
		            UpdateResponse rsp = req.process( server );
		        } catch (IOException e) {
		            e.printStackTrace();
		        }  catch (SolrServerException e){
		            e.printStackTrace();

		        }
		        finally {
		            if (is != null) {
		                try {
		                    is.close();
		                } catch(IOException e) {
		                    e.printStackTrace();
		                }
		            }
		        }
		    }

}
