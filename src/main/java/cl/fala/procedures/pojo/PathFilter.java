package cl.fala.procedures.pojo;

import cl.fala.procedures.Constants;
import cl.fala.procedures.evaluators.PathEvaluatorByExcludingNode;
import cl.fala.procedures.evaluators.PathEvaluatorLabelFilter;
import cl.fala.procedures.evaluators.PathEvaluatorNodeFilters;
import cl.fala.procedures.utilities.StreamUtils;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.TraversalDescription;

import java.util.*;
import java.util.stream.Stream;

public class PathFilter {
    private final Transaction transaction;

    public PathFilter(Transaction transaction) {
        this.transaction = transaction;
    }

    public Stream<Node> getPathEndNode(Node fromNode, Map<String, Object> propertiesFilter) {
        final RelationshipType hasRelation = RelationshipType.withName(Constants.GRAPH_RELATIONSHIPS.HAS);
        TraversalDescription td = transaction.traversalDescription();
        td = td.depthFirst();
        td = td.relationships(hasRelation, Direction.OUTGOING)
                .evaluator(new PathEvaluatorLabelFilter(Label.label(Constants.GRAPH_LABELS.OFFERING)));
        final Iterator<Path> pathIterator = td.traverse(fromNode).iterator();
        return StreamUtils.asStream(pathIterator).map(Path::endNode);
    }
    public boolean hasConnectedPath(Node fromNode, List<Node> toNodes, Direction direction) {
        final RelationshipType hasRelation = RelationshipType.withName(Constants.GRAPH_RELATIONSHIPS.HAS);
        TraversalDescription td = transaction.traversalDescription();
        td =td.depthFirst()
            .relationships(hasRelation, direction)
            .evaluator(new PathEvaluatorNodeFilters(toNodes, false));
        return td.traverse(fromNode).nodes().iterator().hasNext(); 
    }
    public Stream<Node> getPathNodesByExcludingNodes(Node deltaChangeEntity, Node startNode, Direction direction, List<Node> excludeNodes, boolean excludeDeltaChangeEntity) {
        final RelationshipType hasRelation = RelationshipType.withName(Constants.GRAPH_RELATIONSHIPS.HAS);
        
        TraversalDescription td = transaction.traversalDescription();
        td = td.depthFirst()
            .relationships(hasRelation, direction)
            .evaluator(new PathEvaluatorByExcludingNode(excludeNodes, deltaChangeEntity, excludeDeltaChangeEntity));
        final Iterator<Node> nodeIterator = td.traverse(startNode).nodes().iterator();
        return StreamUtils.asStream(nodeIterator);
    }

}
