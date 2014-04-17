	package com.heb.storm.risk;

import java.util.HashMap;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.datastax.demo.utils.PropertyHelper;
import com.heb.storm.risk.cql.CassandraCqlBolt;

public class Main {
    private static final String RISK_SPOUT = "RISK_SPOUT";
    private static final String HIERARCHY_BOLT = "HIERARCHY_BOLT";
    private static final String AGGREGATE_BOLT = "AGGREGATE_BOLT";
    private static final String CASSANDRA_BOLT = "RISK_ANALYTICS_CASSANDRA_BOLT";
	

    public static void main(String[] args) throws Exception {    	
        Config config = new Config();
        
        HashMap<String, Object> clientConfig = new HashMap<String, Object>();
        
        String nodes = PropertyHelper.getProperty("contactPoints", "localhost");
        clientConfig.put("cassandra.nodes", nodes);
        clientConfig.put("cassandra.keyspace", "storm_demo_cql3");        
        
        RiskSpout riskSprout = new RiskSpout();
        RiskNameAggregator riskAggregator = new RiskNameAggregator();
        RiskHierarchyAggregator riskHierarchyAggregator = new RiskHierarchyAggregator();
        
        // create a CassandraBolt that writes to the "stormcf" column
        // family and uses the Tuple field "word" as the row key
        CassandraCqlBolt cassandraBolt = new CassandraCqlBolt();

        // setup topology:
        // wordSpout ==> countBolt ==> cassandraBolt
        TopologyBuilder builder = new TopologyBuilder();

        //Start with SPOUT
        builder.setSpout(RISK_SPOUT, riskSprout, 1);
        
        //Send to 2 bolts
        builder.setBolt(HIERARCHY_BOLT, riskHierarchyAggregator, 1).fieldsGrouping(RISK_SPOUT, new Fields("risk_sensitivity"));
        builder.setBolt(AGGREGATE_BOLT, riskAggregator, 1).fieldsGrouping(RISK_SPOUT, new Fields("risk_sensitivity"));
        
        //Both bolts use the writer bolt
        builder.setBolt(CASSANDRA_BOLT, cassandraBolt, 1).allGrouping(AGGREGATE_BOLT).allGrouping(HIERARCHY_BOLT);

        if (args.length == 0) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", clientConfig, builder.createTopology());
            Thread.sleep(60*1000); //60 mins.
            cluster.killTopology("test");
            cluster.shutdown();
            
        } else {
            config.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], clientConfig, builder.createTopology());
        }
        
        
        Thread.sleep(5*1000);
        System.exit(0); 
    }
}
