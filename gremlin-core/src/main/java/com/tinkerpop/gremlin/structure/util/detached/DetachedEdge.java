package com.tinkerpop.gremlin.structure.util.detached;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.util.DoubleIterator;
import com.tinkerpop.gremlin.process.util.SingleIterator;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Represents an {@link Edge} that is disconnected from a {@link Graph}.  "Disconnection" can mean detachment from
 * a {@link Graph} in the sense that the {@link Edge} was constructed from a {@link Graph} instance and this reference
 * was removed or it can mean that the {@code DetachedEdge} could have been constructed independently of a
 * {@link Graph} instance in the first place.
 * <br/>
 * A {@code DetachedEdge} only has reference to the properties and in/out vertices that are associated with it at the
 * time of detachment (or construction) and is not traversable or mutable.  Note that the references to the in/out
 * vertices are {@link DetachedVertex} instances that only have reference to the
 * {@link com.tinkerpop.gremlin.structure.Vertex#id()} and {@link com.tinkerpop.gremlin.structure.Vertex#label()}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DetachedEdge extends DetachedElement<Edge> implements Edge, Edge.Iterators {

    DetachedVertex outVertex;
    DetachedVertex inVertex;

    public DetachedEdge(final Object id, final String label,
                        final Map<String, Object> properties,
                        final Pair<Object, String> outV,
                        final Pair<Object, String> inV) {
        super(id, label);
        this.outVertex = new DetachedVertex(outV.getValue0(), outV.getValue1());
        this.inVertex = new DetachedVertex(inV.getValue0(), inV.getValue1());
        if (properties != null) this.properties.putAll(convertToDetachedProperty(properties));
    }

    private DetachedEdge() {
    }

    private DetachedEdge(final Edge edge, final boolean asReference) {
        super(edge);
        final Vertex outV = edge.iterators().vertexIterator(Direction.OUT).next();
        final Vertex inV = edge.iterators().vertexIterator(Direction.IN).next();

        // construct a detached vertex here since we don't need properties for DetachedEdge, just the
        // reference to the id and label
        this.outVertex = new DetachedVertex(outV.id(), outV.label());
        this.inVertex = new DetachedVertex(inV.id(), inV.label());

        if (!asReference) {
            edge.iterators().propertyIterator().forEachRemaining(p -> this.properties.put(p.key(), new ArrayList(Arrays.asList(p instanceof DetachedProperty ? p : new DetachedProperty(p, this)))));
        }
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

    @Override
    public Edge attach(final Vertex hostVertex) {
        return StreamFactory.stream(hostVertex.iterators().edgeIterator(Direction.OUT, this.label))
                .filter(edge -> edge.equals(this))
                .findAny().orElseThrow(() -> new IllegalStateException("The detached edge could not be be found incident to the provided vertex: " + this));
    }

    @Override
    public Edge attach(final Graph hostGraph) {
        return hostGraph.iterators().edgeIterator(this.id).next();
    }

    public static DetachedEdge detach(final Edge edge) {
        return detach(edge, false);
    }

    public static DetachedEdge detach(final Edge edge, final boolean asReference) {
        if (null == edge) throw Graph.Exceptions.argumentCanNotBeNull("edge");
        return (edge instanceof DetachedEdge) ? (DetachedEdge) edge : new DetachedEdge(edge, asReference);
    }

    public static Edge addTo(final Graph graph, final DetachedEdge detachedEdge) {
        Iterator<Vertex> itty = graph.iterators().vertexIterator(detachedEdge.outVertex.id());
        Vertex outV = itty.hasNext() ? itty.next() : graph.addVertex(T.id, detachedEdge.outVertex.id());
        itty = graph.iterators().vertexIterator(detachedEdge.inVertex.id());
        Vertex inV = itty.hasNext() ? itty.next() : graph.addVertex(T.id, detachedEdge.inVertex.id());

        if (ElementHelper.areEqual(outV, inV)) {
            final Iterator<Edge> edges = outV.iterators().edgeIterator(Direction.OUT, detachedEdge.label());
            while (edges.hasNext()) {
                final Edge e = edges.next();
                if (ElementHelper.areEqual(detachedEdge, e))
                    return e;
            }
        }

        final Edge e = outV.addEdge(detachedEdge.label(), inV, T.id, detachedEdge.id());
        detachedEdge.properties.entrySet().forEach(kv ->
                        kv.getValue().forEach(p -> e.<Object>property(kv.getKey(), p.value()))
        );

        return e;
    }

    @Override
    public Edge.Iterators iterators() {
        return this;
    }

    @Override
    public GraphTraversal<Edge, Edge> start() {
        throw new UnsupportedOperationException("Detached edges cannot be traversed: " + this);
    }

    private Map<String, List<Property>> convertToDetachedProperty(final Map<String, Object> properties) {
        return properties.entrySet().stream()
                .map(entry -> Pair.with(entry.getKey(), (Property) new DetachedProperty(entry.getKey(), entry.getValue(), this)))
                .collect(Collectors.toMap(p -> p.getValue0(), p -> Arrays.asList(p.getValue1())));
    }

    @Override
    public Iterator<Vertex> vertexIterator(final Direction direction) {
        switch (direction) {
            case OUT:
                return new SingleIterator<>(this.outVertex);
            case IN:
                return new SingleIterator<>(this.inVertex);
            default:
                return new DoubleIterator<>(this.outVertex, this.inVertex);
        }
    }

    @Override
    public <V> Iterator<Property<V>> propertyIterator(final String... propertyKeys) {
        return (Iterator) super.propertyIterator(propertyKeys);
    }
}
