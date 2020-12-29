package com.npd.service;

import java.math.BigDecimal;

public class NPDAttribute {

	private String attributeName;
	private BigDecimal attributeValue;

	public String getAttributeName() {
		return attributeName;
	}

	public void setAttributeName(String attributeName) {
		this.attributeName = attributeName;
	}

	public BigDecimal getAttributeValue() {
		return attributeValue;
	}

	public void setAttributeValue(BigDecimal attributeValue) {
		this.attributeValue = attributeValue;
	}

}
