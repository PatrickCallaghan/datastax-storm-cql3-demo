package com.datastax.demo;

import com.datastax.demo.utils.PropertyHelper;


public class SchemaSetupSingle extends SchemaSetup {
		
	public void setUp(){
		
		String keyspace = PropertyHelper.getProperty("keyspace", "storm_demo_cql3");
		
		DROP_KEYSPACE = "Drop keyspace " +keyspace;
		CREATE_KEYSPACE = "CREATE KEYSPACE " + keyspace + " WITH replication = "
				+ "{'class' : 'SimpleStrategy', 'replication_factor' : 1}";
		
		LOG.info ("Running Single Node DSE setup.");
		
		internalSetup();
		
		LOG.info ("Finished Single Node DSE setup.");
	}

	public static void main(String args[]){
		
		SchemaSetupSingle setup = new SchemaSetupSingle();
		setup.setUp();
		setup.shutdown();
	}
}
