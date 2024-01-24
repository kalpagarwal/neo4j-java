package cl.fala.procedures.pojo;

import java.util.ArrayList;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONString;

import cl.fala.procedures.Constants;
import cl.fala.procedures.Constants.GlobalOperator;

public class Seller {
    private ArrayList<String> values;
    private String operator;
    public Seller(ArrayList<String> sellerValue, String operator) {
        this.values = sellerValue;
        this.operator = operator;
    }
    public Seller() {
        this.values = new ArrayList<String>();
        this.operator = Constants.GlobalOperator.anyOf;
    }
    public ArrayList<String> getValues () {
        return this.values;
    }
    public String getOperator () {
        return this.operator;
    }
    public Boolean isEmpty() {
        return this.values.isEmpty();
    }
    public Boolean filter(String value) {
        boolean status = true;
        if(!this.values.isEmpty()) {
            if(this.operator.equals(GlobalOperator.anyOf)) {
                status = this.values.contains(value);
            } else if(this.operator.equals(GlobalOperator.noneOf)) {
                status = !this.values.contains(value);
            }
        }
        return status;
    }
    public JSONObject toJSON() {
        JSONObject sellerObject = new JSONObject();
        sellerObject.put(Constants.GlobalObjectKeys.value, new JSONArray(this.values));
        sellerObject.put(Constants.GlobalObjectKeys.operator, this.operator);
        return sellerObject;
    }
}
