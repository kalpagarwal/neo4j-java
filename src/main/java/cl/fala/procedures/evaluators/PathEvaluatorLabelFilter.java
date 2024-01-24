package cl.fala.procedures.evaluators;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;


public class PathEvaluatorLabelFilter implements Evaluator {
    private final Label labelFilter;

    public PathEvaluatorLabelFilter(Label labelFilter) {
        this.labelFilter = labelFilter;
    }

    private boolean includeNode(Node node) {
        return node.hasLabel(labelFilter);
    }

    @Override
    public Evaluation evaluate(Path path) {
        if (path != null) {
            if(includeNode(path.endNode())) {
                return Evaluation.INCLUDE_AND_PRUNE;
            } else {
                return Evaluation.EXCLUDE_AND_CONTINUE;
            }
        } else {
            return Evaluation.EXCLUDE_AND_PRUNE;
        }
    }

}
