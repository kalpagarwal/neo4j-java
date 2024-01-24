package cl.fala.procedures.propertyExtractor;

import org.neo4j.graphdb.Node;

public interface NodePropertyExtractorInterface {
    String getProperty(Node node);
}
