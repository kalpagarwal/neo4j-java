package cl.fala.procedures.domainSpecifications.getNodeFromRule;

import java.util.HashSet;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;

import cl.fala.procedures.Constants;

public class GetNodeFromMultiRule implements GetNodeFromRuleInterface{
    private Transaction transaction;

    public GetNodeFromMultiRule(Transaction transaction) {
        this.transaction = transaction;
    }

    @Override
    public Node getNode(JSONObject rule) {
        return null;
    }
    @Override
    public Set<Node> getProductTypeNodes(Node pNode) {
        Set<Node> nodes= new HashSet<>();
        if(pNode.hasLabel(Label.label(Constants.GRAPH_LABELS.PRODUCT_TYPE)) && pNode.hasProperty("productType")) {
            ResourceIterator<Node> productTypeNodes = transaction.findNodes(Label.label("Variant"), "productType", pNode.getProperty("productType"));
            while (productTypeNodes.hasNext()) {
                nodes.add(productTypeNodes.next());
            }
        }
        return nodes;
    }
    @Override
    public Set<Node> getNodes(JSONObject rule) {
        Set<Node> nodes= new HashSet<>();
        JSONObject valueObject = rule.getJSONObject("value");
        JSONArray nodeUuids = valueObject.getJSONArray("multiValue");
        String label = (String) rule.get("label");
        for (int i = 0; i< nodeUuids.length(); i++ ) {
            String nodeUuid = nodeUuids.getString(i);
            Node entityNode = transaction.findNode(Label.label(label), "uuid", nodeUuid);
            if(label.equals("ProductType")) {
                ResourceIterator<Node> productTypeNodes = transaction.findNodes(Label.label("Variant"), "productType", entityNode.getProperty("productType"));
                while (productTypeNodes.hasNext()) {
                    nodes.add(productTypeNodes.next());
                }
            } else {
                nodes.add(entityNode);
            }
        }
        return nodes;
    }
}


