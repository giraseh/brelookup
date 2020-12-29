package com.npd.operation;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;

import com.npd.input.model.LookupStructure;
import com.npd.json.model.Lookup_Layout;
import com.npd.json.model.Lookup_Target;
import com.npd.json.model.esp_cns_mon_full;
import com.npd.service.NPDAttribute;
import com.npd.service.NPDService;
import com.npd.service.NPDServiceImpl;

import hydrograph.engine.spark.components.reusablerow.InputReusableRow;
import hydrograph.engine.spark.components.reusablerow.ReusableRow;
import hydrograph.engine.transformation.base.TransformBase;
import hydrograph.engine.transformation.standardfunctions.LookupFunctions;
import scala.collection.mutable.WrappedArray;

public class process_input implements TransformBase {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final String COMMA = ",";
	private static final String LOOKUP_LAYOUT = "lookup_layout";
	private static final String LOOKUP_NAME = "lookup_name";
	private static final String KEYS = "keys";
	private static final String TARGET = "target";
	private static final String FIELD_POS = "field_position";
	private static final String NAV_FIELD_NAME = "nav_field_name";
	private static final String DEFAULT_VAL = "defaultVal";
	private String PID, lookupId, lookupId2;
	List<LookupStructure> lookupDataList = new ArrayList<>();
	private esp_cns_mon_full esp_cns_mon_full;
	private NPDService npdService;
	private String lookupName;
	private String lookupIndex, lookupKeys;
	private static final String PID_CONST = "PID";
	private static final String LKP_ID = "lookupId";
	private static final String LKP_NAME = "lookupName";
	private static final String LKP_INDEX = "lookupIndex";
	private static final String LKP_ID2 = "lookupId2";
	private static final int TRANSACTION_KEY_SCALE = 5;
	private static final String LKP_KEYS = "lookupKeys";

	@Override
	public void cleanup() {
	}

	@Override
	public void prepare(Properties arg0, List<String> arg1, List<String> arg2) {
		this.PID = arg0.getProperty(PID_CONST);
		this.lookupId = arg0.getProperty(LKP_ID);
		this.lookupName = arg0.getProperty(LKP_NAME);
		this.lookupIndex = arg0.getProperty(LKP_INDEX);
		this.lookupId2 = arg0.getProperty(LKP_ID2);
		this.lookupKeys = arg0.getProperty(LKP_KEYS);

		this.esp_cns_mon_full = readJsonData();

		readLookupData();

		npdService = new NPDServiceImpl(lookupDataList, esp_cns_mon_full);

	}

	@Override
	public void transform(ReusableRow inputRow, ReusableRow outputRow) {

		String[] lookupKeyArr = lookupKeys.split(COMMA);

		ArrayList<BigDecimal> inputKeyList = new ArrayList<>();

		for (String lookupKey : lookupKeyArr) {
			BigDecimal key_value = inputRow.getBigDecimal(lookupKey);
			if (key_value != null)
				inputKeyList.add(key_value);
		}

		if (npdService != null && npdService instanceof NPDServiceImpl)
			((NPDServiceImpl) npdService).setInputKeyList(inputKeyList);

		String[] indexArr = lookupIndex.split(COMMA);

		int[] attributeIndexArr = new int[indexArr.length];

		for (int i = 0; i < indexArr.length; i++) {
			attributeIndexArr[i] = Integer.parseInt(indexArr[i]);
		}

		List<NPDAttribute> npdAttributes = npdService.bre_lookup(lookupName, attributeIndexArr);

		for (NPDAttribute npdAttribute : npdAttributes) {
			String attrName = npdAttribute.getAttributeName();
			BigDecimal attrVal = npdAttribute.getAttributeValue();
			if (attrVal != null)
				outputRow.setField(attrName, attrVal);
			else
				outputRow.setField(attrName, inputRow.getField(attrName));
		}

	}

	private void readLookupData() {
		try {
			int lookupCount = (int) LookupFunctions.lookupCount(PID, lookupId);

			for (int index = 0; index < lookupCount; index++) {
				ReusableRow reusableRow = LookupFunctions.lookupWithIndex(PID, lookupId, index);

				GenericRowWithSchema genericRowWithSchema = (GenericRowWithSchema) ((InputReusableRow) reusableRow)
						.inputRow();

				String lookupName = (String) genericRowWithSchema.get(0);
				String keys = (String) genericRowWithSchema.get(1);
				String outputs = (String) genericRowWithSchema.get(2);

				LookupStructure lookupStructure = new LookupStructure();
				lookupStructure.setLookupName(lookupName);

				String[] keyStrArr = keys.split(COMMA);

				for (int i = 0; i < keyStrArr.length; i++) {

					BigDecimal key = new BigDecimal(keyStrArr[i]);
					key = key.setScale(TRANSACTION_KEY_SCALE);

					lookupStructure.getKeys().add(key);
				}

				String[] outputStrArr = outputs.split(COMMA);

				for (int i = 0; i < outputStrArr.length; i++) {
					lookupStructure.getOutputs().add(new BigDecimal(outputStrArr[i]));
				}

				this.lookupDataList.add(lookupStructure);

			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private esp_cns_mon_full readJsonData() {

		esp_cns_mon_full esp_cns_mon_full_obj = null;

		try {
			int lookupCount = (int) LookupFunctions.lookupCount(PID, lookupId2);
			esp_cns_mon_full_obj = new esp_cns_mon_full();
			for (int index = 0; index < lookupCount; index++) {
				ReusableRow reusableRow = LookupFunctions.lookupWithIndex(PID, lookupId2, index);

				GenericRowWithSchema genericRowWithSchema = (GenericRowWithSchema) ((InputReusableRow) reusableRow)
						.inputRow();

				WrappedArray lookup_layout_obj = (WrappedArray) genericRowWithSchema.getAs(LOOKUP_LAYOUT);

				Object[] lookupLayoutArr = (Object[]) lookup_layout_obj.array();

				for (Object lookupLayout : lookupLayoutArr) {
					Lookup_Layout lookup_Layout = new Lookup_Layout();

					GenericRowWithSchema lookupLayoutSchema = (GenericRowWithSchema) lookupLayout;

					String lookup_name = lookupLayoutSchema.getAs(LOOKUP_NAME);
					lookup_Layout.setLookup_name(lookup_name);

					String keys = lookupLayoutSchema.getAs(KEYS);
					lookup_Layout.setKeys(keys);

					WrappedArray wrappedArrayTarget = (WrappedArray) lookupLayoutSchema.getAs(TARGET);

					Object[] targetArr = (Object[]) wrappedArrayTarget.array();

					for (Object target : targetArr) {
						GenericRowWithSchema targetSchema = (GenericRowWithSchema) target;
						int field_position = targetSchema.getAs(FIELD_POS);
						String nav_field_name = targetSchema.getAs(NAV_FIELD_NAME);
						String defaultVal = targetSchema.getAs(DEFAULT_VAL);

						Lookup_Target lookup_Target = new Lookup_Target();
						lookup_Target.setField_position(field_position);
						lookup_Target.setNav_field_name(nav_field_name);
						lookup_Target.setDefaultVal(defaultVal);

						lookup_Layout.getTargets().add(lookup_Target);

					}
					esp_cns_mon_full_obj.getLookup_layout_list().add(lookup_Layout);
				}

			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return esp_cns_mon_full_obj;
	}

}
