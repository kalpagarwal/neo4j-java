package cl.fala.procedures.pojo;

import cl.fala.procedures.utilities.Parsing;
import org.json.JSONObject;

public class RuleSet {
    private JSONObject ruleSet;

    public RuleSet() {
        this.ruleSet = new JSONObject();
    }

    public RuleSet(JSONObject ruleSet) {
        this.ruleSet = ruleSet;
    }

    public RuleSet(String ruleSetAsBase64) {
        this.ruleSet = new JSONObject(ruleSetAsBase64);
    }

    public JSONObject getRuleSet() {
        return ruleSet;
    }

    public String getAsBase64() {
        return Parsing.encodeToBase64(this.ruleSet.toString());
    }
}
