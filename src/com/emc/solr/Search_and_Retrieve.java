package com.emc.solr;

import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;

public class Search_and_Retrieve {
	public static void main(String[] args) throws MalformedURLException, SolrServerException {
		SolrServer server = new HttpSolrServer("http://localhost:8983/solr");
		SolrQuery query = new SolrQuery();
		if(args[0].equalsIgnoreCase("bank-1")||args[0].equalsIgnoreCase("bank-2")||args[0].equalsIgnoreCase("bank-3"))
				return;
		query.setQuery(args[0]);
		query.setFacet(true);
		query.set("wt", "python");
		query.setRows(5);
		query.setStart(0);

		QueryResponse response = server.query(query);
		SolrDocumentList results = response.getResults();
		/*Connection conn = null;
		Statement stmt = null;
		ArrayList<String> files = new ArrayList<String>();
		try{
		    Class.forName("com.mysql.jdbc.Driver");
		    conn = DriverManager.getConnection("jdbc:mysql://localhost", "root", "hadoop123");
		    stmt = conn.createStatement();
		    stmt.executeUpdate("use user_file");
		    ResultSet rs = stmt.executeQuery("Select * from user_permissions where usrname like "+"'"+args[1]+"'");
		    for(int i=1 ; i < rs.getMetaData().getColumnCount(); i++){
		    	files.add(rs.getString(i));    
		    }
		    
		 }
		 catch(SQLException e){
			    
			    e.printStackTrace();
			 }catch(Exception e){
			    e.printStackTrace();
			 }finally{
			    try{
			       if(stmt!=null)
			          stmt.close();
			    }catch(SQLException se2){
			    }
			    try{
			       if(conn!=null)
			          conn.close();
			    }catch(SQLException se){
			       se.printStackTrace();
			    }
			 }
*/
	    for (int i = 0; i < results.size(); ++i) {
	    		System.out.println(results.get(i).getFieldValue("id"));
	    } 
		
		
	}
} 