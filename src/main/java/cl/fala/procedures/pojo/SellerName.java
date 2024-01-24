package cl.fala.procedures.pojo;

import java.util.ArrayList;

import org.neo4j.graphdb.Node;

import cl.fala.procedures.Constants.GRAPH_NODE_PROPERTIES;
import cl.fala.procedures.Constants.GlobalOperator;

public class SellerName extends Seller{
    private ArrayList<String> values;
    private String operator;
    public SellerName(ArrayList<String> sellerId, String operator) {
        super(sellerId, operator);
        this.values = sellerId;
        this.operator = operator;
    }
    public Boolean filter(Node offering) {
        boolean status = true;
        if(!this.values.isEmpty()) {
            String value = String.valueOf(offering.getProperty(GRAPH_NODE_PROPERTIES.Offering.sellerName, ""));
            if(this.operator.equals(GlobalOperator.anyOf)) {
                status = this.values.contains(value);
            } else if(this.operator.equals(GlobalOperator.noneOf)) {
                status = !this.values.contains(value);
            }
        }
        return status;
    }
}
