package com.npd.service;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.npd.input.model.LookupStructure;
import com.npd.json.model.Lookup_Layout;
import com.npd.json.model.Lookup_Target;
import com.npd.json.model.esp_cns_mon_full;

public class NPDServiceImpl implements NPDService {

	private List<LookupStructure> lookupDataList = new ArrayList<>();
	private esp_cns_mon_full esp_cns_mon_full;
	private List<BigDecimal> inputKeyList = new ArrayList<>();

	public void setInputKeyList(List<BigDecimal> inputKeyList) {
		this.inputKeyList = inputKeyList;
	}

	public NPDServiceImpl(List<LookupStructure> lookupDataList, esp_cns_mon_full esp_cns_mon_full) {
		this.lookupDataList = lookupDataList;
		this.esp_cns_mon_full = esp_cns_mon_full;
	}

	private NPDAttribute getLkpDataSrcOutput(ArrayList<Lookup_Target> lookup_target_list_req,
			LookupStructure lookupDataSrcAvl, int attributeIndex) {
		NPDAttribute npdAttribute = new NPDAttribute();

		if (attributeIndex == lookup_target_list_req.size()) {
			attributeIndex = lookup_target_list_req.size() - 1;
			Lookup_Target lookup_Target = lookup_target_list_req.get(attributeIndex);
			setNPDAttribute(lookup_Target, npdAttribute, lookupDataSrcAvl, attributeIndex);
			return npdAttribute;
		}

		for (Lookup_Target lookup_Target : lookup_target_list_req) {
			int pos = lookup_Target.getField_position();
			if (attributeIndex == pos) {
				setNPDAttribute(lookup_Target, npdAttribute, lookupDataSrcAvl, attributeIndex);
			}
		}

		return npdAttribute;
	}

	private void setNPDAttribute(Lookup_Target lookup_Target, NPDAttribute npdAttribute,
			LookupStructure lookupDataSrcAvl, int attributeIndex) {
		String attributeName = lookup_Target.getNav_field_name();
		npdAttribute.setAttributeName(attributeName);
		BigDecimal outputNPD = null;
		if (lookupDataSrcAvl != null) {
			outputNPD = lookupDataSrcAvl.getOutputs().get(attributeIndex);
		} else {
			String defaultVal = lookup_Target.getDefaultVal();
			if (!defaultVal.isEmpty())
				outputNPD = new BigDecimal(lookup_Target.getDefaultVal());
		}
		npdAttribute.setAttributeValue(outputNPD);

	}

	private Lookup_Layout getReqLkpLayout(String lookupName) {
		Lookup_Layout lookup_layout_req = null;
		ArrayList<Lookup_Layout> lookup_Layouts_Json = esp_cns_mon_full.getLookup_layout_list();

		if (!lookup_Layouts_Json.isEmpty()) {
			for (Lookup_Layout lookup_Layout : lookup_Layouts_Json) {
				String lookupNameJson = lookup_Layout.getLookup_name();
				if (lookupName.equals(lookupNameJson)) {
					lookup_layout_req = lookup_Layout;
					break;
				}
			}
		}

		return lookup_layout_req;
	}

	private LookupStructure getAvlLkpDataSource(String lookupName) {
		LookupStructure lookupDataSrcAvl = null;

		for (LookupStructure lookupDataSrc : lookupDataList) {
			String lookupStructureName = lookupDataSrc.getLookupName();
			if (lookupName.equals(lookupStructureName)) {
				ArrayList<BigDecimal> lookupDataKeys = lookupDataSrc.getKeys();
				if (inputKeyList.equals(lookupDataKeys)) {
					lookupDataSrcAvl = lookupDataSrc;
					break;
				}
			}
		}
		return lookupDataSrcAvl;
	}

	private ArrayList<Lookup_Target> getSortedLookupTargetArr(ArrayList<Lookup_Target> targets) {
		Collections.sort(targets, new Comparator<Lookup_Target>() {

			@Override
			public int compare(Lookup_Target o1, Lookup_Target o2) {
				return o1.getField_position() - o2.getField_position();
			}
		});
		return targets;
	}

	@Override
	public List<NPDAttribute> bre_lookup(String lookupName, int[] attributeIndexArr) {
		List<NPDAttribute> npdAttributes = new ArrayList<>();

		LookupStructure lookupDataSrcAvl = getAvlLkpDataSource(lookupName);

		Lookup_Layout lookup_layout_req = getReqLkpLayout(lookupName);

		if (lookup_layout_req != null) {
			ArrayList<Lookup_Target> lookup_target_list_req = getSortedLookupTargetArr(lookup_layout_req.getTargets());

			for (int attributeIndex : attributeIndexArr) {
				NPDAttribute npdAttribute = getLkpDataSrcOutput(lookup_target_list_req, lookupDataSrcAvl,
						attributeIndex);
				npdAttributes.add(npdAttribute);
			}
		}
		return npdAttributes;
	}

}
