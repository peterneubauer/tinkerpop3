package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.PathConsumer;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AddEdgeStep extends SideEffectStep<Vertex> implements PathConsumer, Reversible {

    public Direction direction;
    public String label;
    public String as;

    public AddEdgeStep(final Traversal traversal, final Direction direction, final String label, final String as, final Object... propertyKeyValues) {
        super(traversal);
        this.direction = direction;
        this.label = label;
        this.as = as;
        super.setConsumer(traverser -> {
            final Vertex currentVertex = traverser.get();
            final Vertex otherVertex = traverser.getPath().get(as);
            if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
                otherVertex.addEdge(label, currentVertex, propertyKeyValues);
            }
            if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
                currentVertex.addEdge(label, otherVertex, propertyKeyValues);
            }
        });
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, this.direction.name(), this.label, this.as);
    }
}
