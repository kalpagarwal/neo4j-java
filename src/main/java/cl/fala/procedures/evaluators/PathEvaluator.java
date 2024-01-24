package cl.fala.procedures.evaluators;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;

import cl.fala.procedures.Constants;

public class PathEvaluator implements Evaluator {
    private final Label labelFilter;
    private final Map<String, Object> nodePropertyFilters;
    private final List<String> sites;
    private boolean siteStatus;

    public PathEvaluator(Label labelFilter, Map<String, Object> nodePropertyFilters) {
        this.labelFilter = labelFilter;
        this.nodePropertyFilters = nodePropertyFilters;
        this.sites = (ArrayList<String>)this.nodePropertyFilters.getOrDefault(Constants.GRAPH_NODE_PROPERTIES.sites, new ArrayList<>());
        this.nodePropertyFilters.remove(Constants.GRAPH_NODE_PROPERTIES.sites);
        this.siteStatus = this.sites.isEmpty();
    }

    private boolean includeNode(Node node) {
        this.siteStatus = this.sites.isEmpty();
        for(String site: this.sites) {
            if(node.hasLabel(Label.label(site))) {
                this.siteStatus = true;
                break;
            }
        }
        if (node.hasLabel(labelFilter) && this.siteStatus) {
            boolean result = true;
            for (String key : this.nodePropertyFilters.keySet()) {
                Object resultKey = this.nodePropertyFilters.get(key);
                if (resultKey instanceof ArrayList) {
                    ArrayList<String> arrayKey = (ArrayList<String>) resultKey;
                    if (!arrayKey.isEmpty()) {
                        String nodeProperty = String.valueOf(node.getProperty(key, ""));
                        result = result && arrayKey.contains(nodeProperty);
                    }
                } else {
                    String filterValue = String.valueOf(this.nodePropertyFilters.get(key));
                    result = result && String.valueOf(node.getProperty(key, "")).equals(filterValue);
                }
            }
            return result;
        } else {
            return false;
        }
    }

    @Override
    public Evaluation evaluate(Path path) {
        if (path != null) {
            boolean nodeIsTarget = includeNode(path.endNode());
            return Evaluation.of(nodeIsTarget, !nodeIsTarget);
        } else {
            return Evaluation.EXCLUDE_AND_PRUNE;
        }
    }

}
