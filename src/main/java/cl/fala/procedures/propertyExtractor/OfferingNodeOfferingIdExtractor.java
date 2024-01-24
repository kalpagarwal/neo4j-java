package cl.fala.procedures.propertyExtractor;

import org.neo4j.graphdb.Node;

public class OfferingNodeOfferingIdExtractor implements NodePropertyExtractorInterface {
    @Override
    public String getProperty(Node node) {
        return (String) node.getProperty("offeringId");
    }
}
