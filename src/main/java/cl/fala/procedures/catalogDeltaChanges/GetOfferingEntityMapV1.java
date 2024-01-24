package cl.fala.procedures.catalogDeltaChanges;

import cl.fala.procedures.Constants;
import cl.fala.procedures.GetOfferings;
import cl.fala.procedures.result.OfferingEntityMapResult;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GetOfferingEntityMapV1 {
    @Context
    public GraphDatabaseService db;
    @Context
    public Transaction tx;
    @Context
    public Log log;

    @Procedure(value = "promodef.getOfferingEntityConnectionMapV1", mode = Mode.READ)
    @Description("promodef.getOfferingEntityConnectionMapV1({" +
            "fromNode::Node," +
            "toNodes::[Node]," +
            "excludePaths::{relationshipIds::[long], excludeFromNode:: Boolean, excludeProductType::String}," +
            "propertiesFilter::{key::string :value:: Object})" +
            ":: result :: Map<String, Map<String, Boolean>>")
    public Stream<OfferingEntityMapResult> getOfferingEntityConnectionMapV1(@Name("config") final Map<String, Object> config) throws Exception {
        Map<String, Object> excludePaths = (Map) config.getOrDefault("excludePaths", new HashMap<>());
        if(excludePaths == null){
            excludePaths = new HashMap<>();
        }
        Map<String, Object> propertiesFilter = (Map<String, Object>) config.getOrDefault("propertiesFilter", new HashMap<>());

        try {
            Node fromNode = (Node)config.get("fromNode");
            List<Node> toNodes = (List<Node>)config.get("toNodes");
            if (!propertiesFilter.containsKey("tenant")) {
                propertiesFilter.put("tenant", String.valueOf(fromNode.getProperty(Constants.GRAPH_NODE_PROPERTIES.tenant)));
            }
            List<Long> relationshipIds = (List<Long>) excludePaths.getOrDefault("relationshipIds", new ArrayList<>());
            Boolean excludeFromNode = (Boolean) excludePaths.getOrDefault("excludeFromNode", false);
            String excludeProductType = (String) excludePaths.getOrDefault("excludeProductType", "");

            return getFromeNodeOfferings(fromNode, toNodes, propertiesFilter, relationshipIds, excludeFromNode, excludeProductType)
                    .map(offeringResult -> {
                        Map<String, Boolean> connectionsMap = (Map<String, Boolean>)offeringResult.get("entityConnectionMap");
                        return new OfferingEntityMapResult(connectionsMap, offeringResult);
                    });
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        } 
    }

    private Stream<Map<String, Object>> getFromeNodeOfferings (Node fromNode, List<Node> toNodes, Map<String, Object> propertiesFilter, List<Long> excludeRelationshipIds, boolean excludeFromNode, String excludeProductType) {
        GetOfferings getOfferings = new GetOfferings(tx);
        return getOfferings.getByQueryFilter(fromNode, propertiesFilter, false)
            .map(result -> {
                Set<Node> toNodesOfferingMap = getOfferings
                            .getByQueryFilter(result, toNodes, Direction.INCOMING, excludeRelationshipIds, false)
                            .collect(Collectors.toSet());

                Map<String, Object> OfferingResult = new HashMap<>();
                for(String property: Constants.OfferingProperties.ReturnProperties) {
                    if(property.equals(Constants.GRAPH_NODE_PROPERTIES.sites)) {
                        OfferingResult.put(property, getSiteLabels(result));
                    } else {
                        OfferingResult.put(property, result.getProperty(property, null));
                    }
                }
                Node excludeNode = excludeFromNode ? fromNode : null;
                OfferingResult.put("entityConnectionMap", getEntityConnectionMap(toNodes,toNodesOfferingMap, excludeNode, excludeProductType));
                return OfferingResult;
            });
    }

    private List<String> getSiteLabels(Node node) {
        Iterable<Label> labels = node.getLabels();
        List<String> siteLabels = new ArrayList<>();
        for(Label label: labels) {
            if(!Constants.ALL_LABELS.Labels.contains(label.name())) {
                siteLabels.add(label.name());
            }
        }
        return siteLabels;
    }
    private Map<String, Object> getEntityConnectionMap(List<Node> toNodes,Set<Node> toNodesOfferingMap, Node excludeNode, String excludeProductType){
        Map<String,Object> entityConnectionMap = new HashMap<>();

        for(Node toNode: toNodes) {
            Map<String, Boolean> statusMap = new HashMap<>();
            statusMap.put("status", true);
            if (toNode.equals(excludeNode)) {
                statusMap.put("status", false);
            } else if(toNodesOfferingMap.contains(toNode)) {
                statusMap.put("status", true);
            } else if(toNode.hasLabel(Label.label(Constants.GRAPH_LABELS.PRODUCT_TYPE))) {
                toNodesOfferingMap.forEach(node -> {
                    if(node.hasLabel(Label.label(Constants.GRAPH_LABELS.VARIANT))) {
                        String pType = toNode.getProperty("productType", "").toString();
                        String variantProType = node.getProperty("productType", "").toString();
                        if(pType.equals(variantProType)){
                            if(!excludeProductType.isEmpty()){
                                if(excludeProductType.equals(pType)){
                                    statusMap.put("status", false);
                                }
                            } else {
                                statusMap.put("status", true);
                            }
                        } else {
                            statusMap.put("status", false);
                        }
                    }
                });
            } else {
                statusMap.put("status", false);
            }
            entityConnectionMap.put(String.valueOf(toNode.getProperty("uuid")), statusMap.get("status"));
        }
        return entityConnectionMap;
    }
}

