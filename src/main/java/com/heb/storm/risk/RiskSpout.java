package com.heb.storm.risk;


import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.heb.finance.analytics.model.RiskSensitivity;

@SuppressWarnings("serial")
public class RiskSpout implements IRichSpout {
	private static final Logger LOG = Logger.getLogger("RiskSensitivityInsertRunner");
	
	private static String[] roots = { "London", "New York", "Hong Kong", "Singapore", "Tokyo", "Paris", "Frankfurt", "Sydney", "Chicago", "Madrid" };
	private static String[] divisions = { "FX", "Equity", "Equity Derivatives", "Bonds", "IRS", "Futures", "CDS", "ABS", "Funds", "Commodities" };
	private static String[] sensitivities = { "irDelta", "irGamma", "irVega", "fxDelta", "fxGamma", "fxVega", "crDelta", "crGamma",
			"crVega", "maturity" };
	private static String DESK = "desk";
	private static String POSITION = "position";
	private static String TRADER = "trader";
	private static Set<String> noOfDistinctPaths = new HashSet<String>();
	
    boolean isDistributed;
    SpoutOutputCollector collector;

    public RiskSpout() {
        this(true);
    }

    public RiskSpout(boolean isDistributed) {
        this.isDistributed = isDistributed;
    }

    public boolean isDistributed() {
        return this.isDistributed;
    }

    @SuppressWarnings("rawtypes")
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    public void close() {

    }

    public void nextTuple() {
    	
    	RiskSensitivity riskSensitivity = this.createNewRandomRiskSensitivity();     	
        this.collector.emit(new Values(riskSensitivity), riskSensitivity.getValue());
        Thread.yield();
    }
    
    private RiskSensitivity createNewRandomRiskSensitivity() {

		String root = roots[(int) (Math.random() * roots.length)];
		String division = divisions[(int) (Math.random() * divisions.length)];
		String sensitivity = sensitivities[(int) (Math.random() * sensitivities.length)];
		String desk = DESK + new Double(Math.random() * 5).intValue();
		String trader = TRADER + new Double(Math.random() * 20).intValue();
		String position = POSITION  + new Double(Math.random() * 20).intValue();

		String path = root + RiskSensitivity.SEPARATOR + division + 
				RiskSensitivity.SEPARATOR + desk + RiskSensitivity.SEPARATOR + trader;

		noOfDistinctPaths.add(path);

		return new RiskSensitivity(sensitivity, path, position, new BigDecimal(Math.random()*10));
	}


    public void ack(Object msgId) {

    }

    public void fail(Object msgId) {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("risk_sensitivity"));
    }

    @Override
    public void activate() {
        // TODO Auto-generated method stub

    }

    @Override
    public void deactivate() {
        // TODO Auto-generated method stub

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
