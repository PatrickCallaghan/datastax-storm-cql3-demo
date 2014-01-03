use storm_demo_cql3; 

CREATE TABLE storm_risk_sensitivities_hierarchy ( 
	hier_path text,
	sub_hier_path text, 
	risk_sens_name text, 
	value double, 
	PRIMARY KEY (hier_path, sub_hier_path, risk_sens_name)
) WITH compaction={'class': 'LeveledCompactionStrategy'};

CREATE TABLE storm_risk_sensitivities_aggregate ( 
	hier_path text,
	risk_sens_name text, 
	value double, 
	PRIMARY KEY (hier_path, risk_sens_name)
) WITH compaction={'class': 'LeveledCompactionStrategy'};

truncate storm_risk_sensitivities_hierarchy;
truncate storm_risk_sensitivities_aggregate;
