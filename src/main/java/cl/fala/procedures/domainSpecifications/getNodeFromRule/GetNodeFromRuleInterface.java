package cl.fala.procedures.domainSpecifications.getNodeFromRule;

import java.util.Set;

import org.json.JSONObject;
import org.neo4j.graphdb.Node;

public interface GetNodeFromRuleInterface {
    Node getNode(JSONObject rule);
    Set<Node> getProductTypeNodes(Node pNode);
    Set<Node> getNodes(JSONObject rule);
}
