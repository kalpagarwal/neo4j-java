package cl.fala.procedures.pathExpanders;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.BranchState;


public class ExcludePathExpander implements PathExpander {
    protected List<Long> excludedRelations;
    protected Direction direction;
    protected RelationshipType relationshipType;

    public ExcludePathExpander(List<Long> excludedRelations, Direction direction, RelationshipType relationshipType) {
        this.excludedRelations = excludedRelations;
        this.direction = direction;
        this.relationshipType = relationshipType;
    }

    @Override
    public Iterable<Relationship> expand(Path path, BranchState state) {
        if(this.excludedRelations.size()>0) {
            List<Relationship> relationsList = new ArrayList<Relationship>();
            if (path != null) {
                Iterator<Relationship> relationIterator = path.endNode().getRelationships(this.direction, this.relationshipType).iterator();
                while(relationIterator.hasNext()) {
                    Relationship r = relationIterator.next();
                    if(!this.excludedRelations.contains(r.getId())) {
                        relationsList.add(r);
                    }
                }
                return relationsList;
            } else {
                return Collections.emptyList();
            }
        } else {
           return path.endNode().getRelationships(this.direction, this.relationshipType);
        }
    }

    @Override
    public PathExpander reverse() {
        // TODO Auto-generated method stub
        return null;
    }
}
