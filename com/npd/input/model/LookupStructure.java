package com.npd.input.model;

import java.math.BigDecimal;
import java.util.ArrayList;

public class LookupStructure {

	private String lookupName;
	private ArrayList<BigDecimal> keys = new ArrayList<>();
	private ArrayList<BigDecimal> outputs = new ArrayList<>();

	public String getLookupName() {
		return lookupName;
	}

	public void setLookupName(String lookupName) {
		this.lookupName = lookupName;
	}

	public ArrayList<BigDecimal> getKeys() {
		return keys;
	}

	/*
	 * public void setKeys(ArrayList<BigDecimal> keys) { this.keys = keys; }
	 */

	public ArrayList<BigDecimal> getOutputs() {
		return outputs;
	}

	/*
	 * public void setOutputs(ArrayList<BigDecimal> outputs) { this.outputs =
	 * outputs; }
	 */
}
