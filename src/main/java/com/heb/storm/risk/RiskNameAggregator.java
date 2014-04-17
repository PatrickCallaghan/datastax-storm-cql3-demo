package com.heb.storm.risk;

import static backtype.storm.utils.Utils.tuple;

import java.util.HashMap;
import java.util.Map;

import org.mortbay.log.Log;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.heb.finance.analytics.model.RiskSensitivity;
import com.heb.storm.risk.cql.CassandraCqlBolt;

@SuppressWarnings("serial")
public class RiskNameAggregator implements IBasicBolt {

	
	// String will contain <hier, map<<sens, double>>>
	private Map<String, Map<String, Double>> aggregatesByRiskType;
	private long count = 0;

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context) {
		this.aggregatesByRiskType = new HashMap<String, Map<String, Double>>();
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		RiskSensitivity riskSensitivity = (RiskSensitivity) input.getValues().get(0);

		if (++count % 1000 == 0) {
			Log.info("Processed " + count);
		}

		String hier = riskSensitivity.getPath();
		String subHier = riskSensitivity.getName();

		//Populate the maps
		addHierValue(riskSensitivity, hier);
		
		// First process the new value
		collector.emit(tuple(CassandraCqlBolt.AGGREGATE, hier, riskSensitivity.getName(), riskSensitivity.getValue().doubleValue()));

		// Process all levels of the hierarchy
		while (hier.contains(RiskSensitivity.SEPARATOR)) {
			subHier = hier.substring(hier.lastIndexOf(RiskSensitivity.SEPARATOR) + 1);
			hier = hier.substring(0, hier.lastIndexOf(RiskSensitivity.SEPARATOR));

			double aggregate = addHierValue(riskSensitivity, hier);

			collector.emit(tuple(CassandraCqlBolt.AGGREGATE, hier, riskSensitivity.getName(), aggregate));
		}
	}

	private double addHierValue(RiskSensitivity riskSensitivity, String hier) {

		double aggregate = 0;

		Map<String, Map<String, Double>> subHierMap;
		Map<String, Double> riskSensitivities;

		// Check for the hierarchy path
		if (this.aggregatesByRiskType.containsKey(hier)) {

			riskSensitivities = aggregatesByRiskType.get(hier);

			// Check for the name
			if (riskSensitivities.containsKey(riskSensitivity.getName())) {
				Double originalValue = riskSensitivities.get(riskSensitivity.getName());
				aggregate = riskSensitivity.getValue().doubleValue() + originalValue.doubleValue();
				riskSensitivities.put(riskSensitivity.getName(), aggregate);
			} else {
				riskSensitivities.put(riskSensitivity.getName(), riskSensitivity.getValue().doubleValue());
			}

		} else {
			// Create new maps to hold values
			riskSensitivities = new HashMap<String, Double>();
			aggregate = riskSensitivity.getValue().doubleValue();
			riskSensitivities.put(riskSensitivity.getName(), aggregate);
			this.aggregatesByRiskType.put(hier, riskSensitivities);
		}
		return aggregate;
	}

	public void cleanup() {

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("Type", "hierarchy_path", "risk_sensitivity", "aggregate"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}