package com.tinkerpop.gremlin.tinkergraph.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GraphStep;
import com.tinkerpop.gremlin.process.TraverserGenerator;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.process.util.TraversalMetrics;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.HasContainer;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerHelper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphStep<E extends Element> extends GraphStep<E> {

    public final List<HasContainer> hasContainers = new ArrayList<>();

    public TinkerGraphStep(final Traversal traversal, final Class<E> returnClass) {
        super(traversal, returnClass);
    }

    @Override
    public void generateTraversers(final TraverserGenerator traverserGenerator) {
        if (PROFILING_ENABLED) TraversalMetrics.start(this);
        this.start = Vertex.class.isAssignableFrom(this.returnClass) ? this.vertices() : this.edges();
        this.starts.add(traverserGenerator.generateIterator((Iterator<E>) this.start, this));
        if (PROFILING_ENABLED) TraversalMetrics.stop(this);
    }

    private Iterator<? extends Edge> edges() {
        final HasContainer indexedContainer = getIndexKey(Edge.class);
        final Stream<? extends Edge> edgeStream = (null == indexedContainer) ?
                TinkerHelper.getEdges((TinkerGraph)this.traversal.sideEffects().getGraph()).stream() :
                TinkerHelper.queryEdgeIndex((TinkerGraph)this.traversal.sideEffects().getGraph(), indexedContainer.key, indexedContainer.value).stream();

        // the copy to a new List is intentional as remove() operations will cause ConcurrentModificationException otherwise
        return edgeStream.filter(e -> HasContainer.testAll(e, hasContainers)).collect(Collectors.<Edge>toList()).iterator();
    }

    private Iterator<? extends Vertex> vertices() {
        final HasContainer indexedContainer = getIndexKey(Vertex.class);
        final Stream<? extends Vertex> vertexStream = (null == indexedContainer) ?
                TinkerHelper.getVertices((TinkerGraph)this.traversal.sideEffects().getGraph()).stream() :
                TinkerHelper.queryVertexIndex((TinkerGraph)this.traversal.sideEffects().getGraph(), indexedContainer.key, indexedContainer.value).stream();

        // the copy to a new List is intentional as remove() operations will cause ConcurrentModificationException otherwise
        return vertexStream.filter(v -> HasContainer.testAll(v, this.hasContainers)).collect(Collectors.<Vertex>toList()).iterator();
    }

    private HasContainer getIndexKey(final Class<? extends Element> indexedClass) {
        final Set<String> indexedKeys = ((TinkerGraph)this.traversal.sideEffects().getGraph()).getIndexedKeys(indexedClass);
        return this.hasContainers.stream()
                .filter(c -> indexedKeys.contains(c.key) && c.predicate.equals(Compare.eq))
                .findAny()
                .orElseGet(() -> null);
    }

    public String toString() {
        return this.hasContainers.isEmpty() ? super.toString() : TraversalHelper.makeStepString(this, this.hasContainers);
    }

}
