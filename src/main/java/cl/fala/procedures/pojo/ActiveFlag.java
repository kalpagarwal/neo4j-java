package cl.fala.procedures.pojo;

import org.neo4j.graphdb.Node;

import cl.fala.procedures.Constants;

import org.json.JSONObject;
import org.neo4j.graphdb.Label;


public class ActiveFlag {
    private boolean checkIsActiveFlag;
    public ActiveFlag (boolean checkIsActiveFlag){
        this.checkIsActiveFlag = checkIsActiveFlag;
    }
    public JSONObject toJSON() {
        JSONObject sellerObject = new JSONObject();
        sellerObject.put(Constants.GlobalObjectKeys.checkIsActiveFlag, this.checkIsActiveFlag);
        return sellerObject;
    }

    public Boolean filter(Node offering) {
        boolean status = true;
        if(this.checkIsActiveFlag) {
            status = Boolean.valueOf(
                String.valueOf(
                    offering.getProperty(Constants.GRAPH_NODE_PROPERTIES.Offering.isActive, true)
                )
            );
        }
        return status;
    }
}
