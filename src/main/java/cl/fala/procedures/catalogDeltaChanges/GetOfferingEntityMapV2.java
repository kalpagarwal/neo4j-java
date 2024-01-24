package cl.fala.procedures.catalogDeltaChanges;

import cl.fala.functions.GenerateUUID;
import cl.fala.procedures.Constants;
import cl.fala.procedures.pojo.PathFilter;
import cl.fala.procedures.result.OfferingEntityMapResult;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GetOfferingEntityMapV2 {
    @Context
    public GraphDatabaseService db;
    @Context
    public Transaction tx;
    @Context
    public Log log;

    @Procedure(value = "promodef.getOfferingEntityConnectionMapV2", mode = Mode.READ)
    @Description("promodef.getOfferingEntityConnectionMapV2({" +
            "fromNode::Node," +
            "includeNodes::[Node]"+
            "excludePaths::{excludeNodes::[Node], excludeFromNode:: Boolean, excludeProductType::String}" +
            "})" +
            ":: result :: Map<String, Map<String, Boolean>>")
    public Stream<OfferingEntityMapResult> getOfferingEntityConnectionMapV2(@Name("config") final Map<String, Object> config) throws Exception {
        @SuppressWarnings("unchecked")
        Map<String, Object> excludePaths = (Map<String, Object>) config.getOrDefault("excludePaths", new HashMap<>());
        

        Map<String, Object> propertiesFilter = new HashMap<>();

        try {
            Node fromNode = (Node)config.get("fromNode");
            @SuppressWarnings("unchecked")
            List<Node> excludeNodes = (List<Node>) excludePaths.getOrDefault("excludeNodes", new ArrayList<>());
            Boolean excludeFromNode = (Boolean) excludePaths.getOrDefault("excludeFromNode", false);
            String excludeProductType = (String) excludePaths.getOrDefault("excludeProductType", "");
            @SuppressWarnings("unchecked")
            List<Node> includeNodes = (List<Node>) config.getOrDefault("includeNodes", new ArrayList<>());

            return getFromeNodeOfferings(fromNode, excludeNodes, propertiesFilter, excludeFromNode, excludeProductType, includeNodes)
                    .map(offeringResult -> {
                        @SuppressWarnings("unchecked")
                        Map<String, Boolean> connectionsMap = (Map<String, Boolean>)offeringResult.get("entityConnectionMap");
                        return new OfferingEntityMapResult(connectionsMap, offeringResult);
                    });
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    private Stream<Map<String, Object>> getFromeNodeOfferings (Node fromNode, List<Node> excludeNodes, Map<String, Object> propertiesFilter, boolean excludeFromNode, String excludeProductType, List<Node> includeNodes) {
        PathFilter pathFilter = new PathFilter(tx);
        List<Node> filterNodes = new ArrayList<>();
        filterNodes.addAll(excludeNodes);
        filterNodes.addAll(includeNodes);
        return pathFilter.getPathEndNode(fromNode, propertiesFilter)
            .filter(offering -> {
                if(filterNodes.isEmpty()){
                    return true;
                } else if(pathFilter.hasConnectedPath(offering, filterNodes, Direction.INCOMING)){
                    return true;
                } else {
                    return false;
                }
            })
            .map(offering -> {
                Set<Node> offeringEntityMap = pathFilter
                            .getPathNodesByExcludingNodes(fromNode, offering, Direction.INCOMING, excludeNodes, excludeFromNode)
                            .collect(Collectors.toSet());

                Map<String, Object> OfferingResult = new HashMap<>();
                for(String property: Constants.OfferingProperties.ReturnProperties) {
                    if(property.equals(Constants.GRAPH_NODE_PROPERTIES.sites)) {
                        OfferingResult.put(property, getSiteLabels(offering));
                    } else if(property.equals(Constants.GRAPH_NODE_PROPERTIES.Offering.isActive)) {
                        OfferingResult.put(property, offering.getProperty(property, true));
                    } else {
                        OfferingResult.put(property, offering.getProperty(property, null));
                    }
                }
                Node excludeNode = excludeFromNode ? fromNode : null;
                OfferingResult.put("entityConnectionMap", getEntityConnectionMap(offeringEntityMap, excludeNode, excludeProductType));
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

    private Map<String, HashMap<String, Boolean>> getEntityConnectionMap(Set<Node> toNodesOfferingMap, Node excludeNode, String excludeProductType){
        HashMap<String, HashMap<String, Boolean>> entityConnectionMap = new HashMap<String, HashMap<String,Boolean>>();
        toNodesOfferingMap.forEach(node -> {
            boolean status = true;
            Iterable<Label> labels = node.getLabels();
            for(Label label: labels) {
                if(Constants.ALL_LABELS.Labels.contains(label.name())) {
                    if(!entityConnectionMap.containsKey(label.name())) {
                        entityConnectionMap.put(label.name(), new HashMap<String, Boolean>());
                    }
                    entityConnectionMap.get(label.name()).put(String.valueOf(node.getProperty("uuid", "defaultUuid")), status);
                }
            }    
            if(node.hasLabel(Label.label(Constants.GRAPH_LABELS.VARIANT))) {
                List<String> uuidList = new ArrayList<>(2);
                String variantProType = node.getProperty("productType", "").toString();
                String tenant = node.getProperty("tenant", "").toString();
                uuidList.add(variantProType);
                uuidList.add(tenant);
                if(variantProType != "" && tenant != null) {
                    try {
                        String productTypeUUid = new GenerateUUID().generateUUIDV5(uuidList, "");
                        if(!entityConnectionMap.containsKey(Constants.GRAPH_LABELS.PRODUCT_TYPE)) {
                            entityConnectionMap.put(Constants.GRAPH_LABELS.PRODUCT_TYPE, new HashMap<String, Boolean>());
                        }
                        entityConnectionMap.get(Constants.GRAPH_LABELS.PRODUCT_TYPE).put(productTypeUUid, !excludeProductType.equals(productTypeUUid));
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        return entityConnectionMap;
    }
}

// const neo4j = require('neo4j-driver');

// const driver = neo4j.driver('bolt://localhost:7687',
// neo4j.auth.basic('username', 'password'));

// async function getOfferingEntityConnectionMapV2(config) {
// const session = driver.session();

// try {
// const excludePaths = config.excludePaths || {};
// const fromNode = config.fromNode;
// const excludeNodes = excludePaths.excludeNodes || [];
// const excludeFromNode = excludePaths.excludeFromNode || false;
// const excludeProductType = excludePaths.excludeProductType || '';
// const includeNodes = config.includeNodes || [];

// const results = await getFromNodeOfferings(session, fromNode, excludeNodes,
// {}, excludeFromNode, excludeProductType, includeNodes);

// return results.map(offeringResult => ({
// entityConnectionMap: offeringResult.entityConnectionMap,
// // Add other properties if needed
// }));
// } finally {
// await session.close();
// }
// }

// async function getFromNodeOfferings(session, fromNode, excludeNodes,
// propertiesFilter, excludeFromNode, excludeProductType, includeNodes) {
// const pathFilter = new PathFilter(session);
// const filterNodes = excludeNodes.concat(includeNodes);

// const offeringResults = await pathFilter.getPathEndNode(session, fromNode,
// propertiesFilter)
// .then(offerings => offerings.filter(offering => {
// if (filterNodes.length === 0) {
// return true;
// } else if (pathFilter.hasConnectedPath(offering, filterNodes, 'INCOMING')) {
// return true;
// } else {
// return false;
// }
// }))
// .then(offerings => Promise.all(offerings.map(offering =>
// getOfferingEntityMap(session, offering, fromNode, excludeNodes,
// excludeFromNode, excludeProductType)
// )));

// return offeringResults;
// }

// async function getOfferingEntityMap(session, offering, fromNode,
// excludeNodes, excludeFromNode, excludeProductType) {
// const pathFilter = new PathFilter(session);
// const offeringEntityMap = await
// pathFilter.getPathNodesByExcludingNodes(session, fromNode, offering,
// 'INCOMING', excludeNodes, excludeFromNode);

// const offeringResult = {
// // Add other properties if needed
// entityConnectionMap: getEntityConnectionMap(offeringEntityMap, excludeNode,
// excludeProductType),
// };

// return offeringResult;
// }

// function getEntityConnectionMap(toNodesOfferingMap, excludeNode,
// excludeProductType) {
// const entityConnectionMap = {};

// toNodesOfferingMap.forEach(node => {
// // Process each node and update entityConnectionMap accordingly
// });

// return entityConnectionMap;
// }

// // You may need to implement the necessary logic for the remaining parts of
// the code.

// // Close the Neo4j driver when done
// process.on('exit', () => driver.close());
