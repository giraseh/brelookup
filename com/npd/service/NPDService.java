package com.npd.service;

import java.util.List;

public interface NPDService {

	List<NPDAttribute> bre_lookup(String lookupName, int attributeIndexArr[]);

}
