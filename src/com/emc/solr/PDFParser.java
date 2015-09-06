package com.emc.solr;

	
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

public class PDFParser {
	private HttpSolrServer server = null;

public static void main(String[] args){
	
			String url = "http://192.168.200.135:8983/solr";
			HttpSolrServer server = new HttpSolrServer(url);
	        InputStream is = null;
	        String path = "DocumentLibrary.pdf";

	        try {
	            is = new BufferedInputStream(new FileInputStream(new File(path)));
	            
	            Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
	            //server.deleteById("");
	            //server.commit();
	            SolrInputDocument document = new SolrInputDocument();
	            document.addField("id","har://hdfs-localhost:8020/user/notroot/outputpdf/pdf.har/"+path);
	            System.out.println(document);
	            Parser parser = new AutoDetectParser();
	            String content="";
	            ContentHandler handler = new BodyContentHandler();
	            
	            	
	            Metadata metadata = new Metadata();
	            
	            System.out.println("Parser starts");
	            parser.parse(is, handler, metadata, new ParseContext());
	            
	            ContentHandlerDecorator cd = new ContentHandlerDecorator(handler);
	            content = cd.toString();
	            StringTokenizer contentTokens = new StringTokenizer(content);
	            while(contentTokens.hasMoreTokens()) {
	            	document.addField("content",contentTokens.nextToken());
	            }
	            
	            //System.out.println("document:\t" + document);
	            System.out.println("the content is \t" + content);
	            System.out.println("Parser ends");
	            System.out.println("document:\t" + document);
	            for (String name : metadata.names()) {
	                String value = metadata.get(name);

	                if (value != null) {
	                    System.out.println("Metadata Name:  " + name);
	                    System.out.println("Metadata Value: " + value);
	                }
	            }
	            docs.add(document);
	            server.add(docs);
	            server.commit();
	            
	            UpdateRequest req = new UpdateRequest();
	            req.setAction( UpdateRequest.ACTION.COMMIT, false, false );
	            req.add( docs );
	            UpdateResponse rsp = req.process( server );
	        } catch (IOException e) {
	            e.printStackTrace();
	        } catch (TikaException e) {
	            e.printStackTrace();
	        } catch (SAXException e) {
	            e.printStackTrace();
	        } catch (SolrServerException e){
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


