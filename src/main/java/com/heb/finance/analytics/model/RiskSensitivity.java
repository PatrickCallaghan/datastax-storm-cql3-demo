package com.heb.finance.analytics.model;

import java.io.Serializable;
import java.math.BigDecimal;

public class RiskSensitivity implements Serializable{

	public final static String SEPARATOR = "/";
	
	public final String name;
	public final String path;
	public final String position;
	public final BigDecimal value;

	public RiskSensitivity(String name, String path, String position, BigDecimal value) {
		super();
		this.name = name;
		this.path = path;
		this.position = position;
		this.value = value;
	}

	public String getName() {
		return name;
	}

	public String getPath() {
		return path;
	}

	public BigDecimal getValue() {
		return value;
	}
	
	public String getPosition(){
		return position;
	}

	@Override
	public String toString() {
		return "RiskSensitivity [name=" + name + ", path=" + path + ", position=" + position + ", value=" + value + "]";
	}
}
