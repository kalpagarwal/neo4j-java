package cl.fala.procedures;

import cl.fala.procedures.evaluators.PathEvaluator;
import cl.fala.procedures.evaluators.PathEvaluatorNodeFilters;
import cl.fala.procedures.pathExpanders.ExcludePathExpander;
import cl.fala.procedures.pojo.GlobalFilter;
import cl.fala.procedures.propertyExtractor.NodePropertyExtractorInterface;
import cl.fala.procedures.utilities.StreamUtils;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.TraversalDescription;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GetOfferings {
    private final Transaction transaction;

    public GetOfferings(Transaction transaction) {
        this.transaction = transaction;
    }

    public Stream<Node> getByQueryFilter(Node fromNode, Map<String, Object> propertiesFilter, boolean bfs) {
        final RelationshipType hasRelation = RelationshipType.withName(Constants.GRAPH_RELATIONSHIPS.HAS);
        TraversalDescription td = transaction.traversalDescription();
        td = bfs ? td.breadthFirst() : td.depthFirst();
        td = td.relationships(hasRelation, Direction.OUTGOING)
                .evaluator(new PathEvaluator(Label.label(Constants.GRAPH_LABELS.OFFERING), propertiesFilter));
        final Iterator<Path> pathIterator = td.traverse(fromNode).iterator();
        return StreamUtils.asStream(pathIterator).map(Path::endNode);
    }
    public Stream<Node> getByQueryFilter(Node fromNode, List<Node> nodesFilter, Direction direction, List<Long> excludeRelationship, Boolean bfs) {
        final RelationshipType hasRelation = RelationshipType.withName(Constants.GRAPH_RELATIONSHIPS.HAS);
        ExcludePathExpander pathExpander = new ExcludePathExpander(excludeRelationship, direction, hasRelation);
        boolean hasProductTypeFilter = false;
        for(int i = 0; i< nodesFilter.size(); i++){
            if(!hasProductTypeFilter) {
                hasProductTypeFilter = nodesFilter.get(i).hasLabel(Label.label(Constants.GRAPH_LABELS.PRODUCT_TYPE));
            }
        }
        TraversalDescription td = transaction.traversalDescription();
        td = bfs ? td.breadthFirst() : td.depthFirst();
        td = td.expand(pathExpander)
                .evaluator(new PathEvaluatorNodeFilters(nodesFilter, hasProductTypeFilter));
        final Iterator<Path> pathIterator = td.traverse(fromNode).iterator();
        return StreamUtils.asStream(pathIterator).map(Path::endNode);
    }

    public Stream<String> getAll(NodePropertyExtractorInterface nodePropertyExtractorInterface, Map<String, String> queryFilters, GlobalFilter globalfilter) {
        Label offeringLabel = Label.label(Constants.GRAPH_LABELS.OFFERING);
        
        Map<String, Object> queryFilter = new HashMap<>();
        queryFilter.put("tenant", queryFilters.get("tenant"));
        try (ResourceIterator<Node> iterator = transaction.findNodes(offeringLabel, queryFilter)) {
            return iterator.stream().filter(globalfilter::filter)
                    .map(nodePropertyExtractorInterface::getProperty).collect(Collectors.toSet()).stream();
        }
    }

}
