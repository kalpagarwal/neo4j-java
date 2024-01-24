package cl.fala.procedures.result;

import java.util.HashMap;
import java.util.Map;

public class OfferingEntityMapResult {
    public Map<String, Object> result;

    public OfferingEntityMapResult(String offeringId, Map<String, Boolean> entityConnectionMap, String sellerId) {
        this.result = new HashMap<>();
        result.put("offeringId", offeringId);
        result.put("sellerId", sellerId);
        result.put("entityConnectionMap", entityConnectionMap);
    }
    public OfferingEntityMapResult(Map<String, Boolean> entityConnectionMap, Map<String,Object> propertyMap) {
        this.result = new HashMap<>();
        result.put("entityConnectionMap", entityConnectionMap);
        result.putAll(propertyMap);
    }
}
