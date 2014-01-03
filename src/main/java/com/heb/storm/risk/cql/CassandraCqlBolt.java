package com.heb.storm.risk.cql;


import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.mortbay.log.Log;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

public class CassandraCqlBolt extends BaseRichBolt {

    public static final int AGGREGATE = 1;
    public static final int HIERARCHY= 2;
    
	private OutputCollector outputCollector;
    private Cluster cassandraCluster;
    private Session session;
    private Map<String,String> config;
    
    private String cqlHier = "insert into storm_risk_sensitivities_hierarchy (hier_path, sub_hier_path, risk_sens_name, value) VALUES (?, ?, ?, ?)";
    private String cqlAgg = "insert into storm_risk_sensitivities_aggregate (hier_path, risk_sens_name, value) VALUES (?, ?, ?)";    
    private PreparedStatement insertHierStmt;
    private PreparedStatement insertAggStmt;

    public static Session getSessionWithRetry(Cluster cluster, String keyspace) {
        while (true) {
            try {
                return cluster.connect(keyspace);
            } catch (NoHostAvailableException e) {
                Log.warn("All Cassandra Hosts offline. Waiting to try again.");
                Utils.sleep(1000);
            }
        }
    }

    public static Cluster setupCassandraClient(String []nodes) {
        return Cluster.builder()
                .withoutJMXReporting()
                .withoutMetrics()
                .addContactPoints(nodes)
                .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
                .withReconnectionPolicy(new ExponentialReconnectionPolicy(100L, TimeUnit.MINUTES.toMillis(5)))
                .withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()))
                .build();
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    	
        this.outputCollector = outputCollector;
        config = (Map<String,String>) map;

        cassandraCluster = setupCassandraClient(config.get("cassandra.nodes").split(","));             
        session = CassandraCqlBolt.getSessionWithRetry(cassandraCluster, (String)config.get("cassandra.keyspace"));
       
        //Prepare the statement
        insertHierStmt = session.prepare(cqlHier);
        insertAggStmt = session.prepare(cqlAgg);
        
        insertHierStmt.setConsistencyLevel(ConsistencyLevel.ONE);
        insertAggStmt.setConsistencyLevel(ConsistencyLevel.ONE);
    }

    @Override
    public void execute(Tuple input) {
        try {
        	int type = input.getInteger(0);
        	
        	if (type == HIERARCHY){
        		String hierarchyPath = input.getString(1);
        		String subHierarchyPath = input.getString(2);
        		String riskSensitivity = input.getString(3);
        		Double value =  new Double(input.getDouble(4));        	
        	                                
        		BoundStatement boundHier = insertHierStmt.bind(new Object[]{hierarchyPath, subHierarchyPath, riskSensitivity, value});
            
        		session.execute(boundHier);
        	}else if (type == AGGREGATE){
        		String hierarchyPath = input.getString(1);
        		String riskSensitivity = input.getString(2);
        		Double value =  new Double(input.getDouble(3));        	
        	                                
        		BoundStatement boundAgg = insertAggStmt.bind(new Object[]{hierarchyPath, riskSensitivity, value});
            
        		session.execute(boundAgg);
        	}
            
            outputCollector.ack(input);
           
        } catch (Throwable t) {
            outputCollector.reportError(t);
            outputCollector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //No outputs
    }
}