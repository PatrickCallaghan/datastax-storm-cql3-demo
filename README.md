# Realtime Risk Aggregator with Storm Demo

This is a demo to show how Cassandra and Storm can be used to provide realtime aggregation of risk sensitivities. On a single machine it should process approx 5000 inserts per second while aggregating parts of the hierarchy. 

## The problem

The problem is the same as from the https://github.com/PatrickCallaghan/datastax-risksensitivity-demo project.

We want to be able to run the following types of queries

    select * from storm_risk_sensitivities_hierarchy  where hier_path = 'Paris/FX/desk4';

    select * from storm_risk_sensitivities_hierarchy  where hier_path = 'Paris/FX/desk4' and sub_hier_path='trader3';
    
    select * from storm_risk_sensitivities_hierarchy  where hier_path = 'Paris/FX/desk4' and sub_hier_path='trader3' and risk_sens_name='irDelta';

In this demo we also want to be able to aggregate all the sensitivities at a certain level. So we will want to be able to use the following queries.

    select * from storm_risk_sensitivities_aggregate  where hier_path = 'Paris/FX' and risk_sens_name = 'crDelta';
    
    select * from storm_risk_sensitivities_aggregate  where hier_path = 'Paris/FX/desk1' and risk_sens_name = 'maturity';

## Storm

This demo uses storm to populate the 2 tables needed to fulfil our queries. One is the hierarchy table and one is the aggreate table. 

The topology consists of 

	RISK SPOUT - creates an endless stream of sensitivity values for a certain position. 
	
	HIERARCHY BOLT - inserts the sensitivity value for the position hierarhy. This also aggregates the values up to each parent hierarhcy for each sub hierarchy. e.g. London/FX for each desk
	AGGREATE BOLT - aggregates the values up to each parent hierarhcy e.g. London/FX for irDelta 
	
	CASSANDRA BOLT - a writer for both the HIERARCHY and AGGREGATE BOLTS. Uses cql 3 to insert the values to cassandra.

## Running the demo 

You will need a java runtime (preferably 7) along with maven 3 to run this demo. You will need to be comfortable installing and starting Cassandra and DSE (hadoop and solr nodes included). Start DSE or a cassandra instance on your local machine.

This demo uses quite a lot of memory so it is worth setting the MAVEN_OPTS to run maven with more memory

    export MAVEN_OPTS=-Xmx512M

## Schema Setup
Note : This will drop the keyspace and create a new one. All existing data will be lost. 

To specify contact points use the contactPoints command line parameter e.g. 

To create the a single node cluster with replication factor of 1 for standard localhost setup, run the following

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaSetupSingle"

To run the insert

    mvn clean compile exec:java -Dexec.mainClass=com.heb.storm.risk.StormRiskAnalyticsCqlAggregator
		
The stopSize property allows us to specify the number of inserts we want to run. 
