package cl.fala.procedures.pojo;

import java.util.ArrayList;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

import cl.fala.procedures.Constants.GlobalOperator;

public class Sites extends Seller{
    private ArrayList<String> values;
    private String operator;
    public Sites(ArrayList<String> sites, String operator) {
        super(sites, operator);
        this.values = sites;
        this.operator = operator;
    }
    public Boolean filter(Node offering) {
        boolean status = true;
        if(!this.values.isEmpty()) {
            ArrayList<String> lables = new ArrayList<>();
            for(Label label: offering.getLabels()) {
                lables.add(label.name());
            }
            lables.retainAll(this.values);
            if(this.operator.equals(GlobalOperator.anyOf)) {
                status = lables.size()>0;
            } else if(this.operator.equals(GlobalOperator.noneOf)) {
                status = lables.size()==0;
            }
        }
        return status;
    }
}
