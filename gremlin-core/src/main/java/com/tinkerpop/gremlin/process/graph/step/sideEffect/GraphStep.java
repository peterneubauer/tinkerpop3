package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraverserGenerator;
import com.tinkerpop.gremlin.process.graph.marker.TraverserSource;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.process.util.TraversalMetrics;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphStep<E extends Element> extends StartStep<E> implements TraverserSource {

    protected final Class<E> returnClass;

    public GraphStep(final Traversal traversal, final Class<E> returnClass) {
        super(traversal);
        this.returnClass = returnClass;
    }


    public boolean returnsVertices() {
        return Vertex.class.isAssignableFrom(this.returnClass);
    }

    public boolean returnsEdges() {
        return Edge.class.isAssignableFrom(this.returnClass);
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.returnClass.getSimpleName().toLowerCase());
    }

    @Override
    public void generateTraversers(final TraverserGenerator traverserGenerator) {
        if (PROFILING_ENABLED) TraversalMetrics.start(this);
        this.start = Vertex.class.isAssignableFrom(this.returnClass) ?
                this.traversal.sideEffects().getGraph().iterators().vertexIterator() :
                this.traversal.sideEffects().getGraph().iterators().edgeIterator();
        this.starts.add(traverserGenerator.generateIterator((Iterator<E>) this.start, this));
        if (PROFILING_ENABLED) TraversalMetrics.stop(this);
    }
}
