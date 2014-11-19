package com.tinkerpop.gremlin.structure.util.detached;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.FeatureRequirement;
import com.tinkerpop.gremlin.FeatureRequirementSet;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.javatuples.Pair;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData;
import static org.junit.Assert.*;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DetachedEdgeTest extends AbstractGremlinTest {

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotConstructWithNullElement() {
        DetachedEdge.detach(null);
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldNotConstructNewWithSomethingAlreadyDetached() {
        final Vertex v = g.addVertex();
        final Edge e = v.addEdge("test", v);
        final DetachedEdge de = DetachedEdge.detach(e);
        assertSame(de, DetachedEdge.detach(de));
    }

    @Test
    @LoadGraphWith(GraphData.MODERN)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldConstructDetachedEdge() {
        g.e(convertToEdgeId("marko", "knows", "vadas")).next().property(Graph.Key.hide("year"), 2002);
        final DetachedEdge detachedEdge = DetachedEdge.detach(g.e(convertToEdgeId("marko", "knows", "vadas")).next());
        assertEquals(convertToEdgeId("marko", "knows", "vadas"), detachedEdge.id());
        assertEquals("knows", detachedEdge.label());
        assertEquals(DetachedVertex.class, detachedEdge.iterators().vertexIterator(Direction.OUT).next().getClass());
        assertEquals(convertToVertexId("marko"), detachedEdge.iterators().vertexIterator(Direction.OUT).next().id());
        assertEquals("person", detachedEdge.iterators().vertexIterator(Direction.IN).next().label());
        assertEquals(DetachedVertex.class, detachedEdge.iterators().vertexIterator(Direction.IN).next().getClass());
        assertEquals(convertToVertexId("vadas"), detachedEdge.iterators().vertexIterator(Direction.IN).next().id());
        assertEquals("person", detachedEdge.iterators().vertexIterator(Direction.IN).next().label());

        assertEquals(2, StreamFactory.stream(detachedEdge.iterators().propertyIterator()).count());
        assertEquals(1, StreamFactory.stream(detachedEdge.iterators().propertyIterator(Graph.Key.hide("year"))).count());
        assertEquals(0.5d, detachedEdge.iterators().propertyIterator("weight").next().value());
    }

    @Test
    @LoadGraphWith(GraphData.MODERN)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldConstructDetachedEdgeAsReference() {
        g.e(convertToEdgeId("marko", "knows", "vadas")).next().property(Graph.Key.hide("year"), 2002);
        final DetachedEdge detachedEdge = DetachedEdge.detach(g.e(convertToEdgeId("marko", "knows", "vadas")).next(), true);
        assertEquals(convertToEdgeId("marko", "knows", "vadas"), detachedEdge.id());
        assertEquals("knows", detachedEdge.label());
        assertEquals(DetachedVertex.class, detachedEdge.iterators().vertexIterator(Direction.OUT).next().getClass());
        assertEquals(convertToVertexId("marko"), detachedEdge.iterators().vertexIterator(Direction.OUT).next().id());
        assertEquals("person", detachedEdge.iterators().vertexIterator(Direction.IN).next().label());
        assertEquals(DetachedVertex.class, detachedEdge.iterators().vertexIterator(Direction.IN).next().getClass());
        assertEquals(convertToVertexId("vadas"), detachedEdge.iterators().vertexIterator(Direction.IN).next().id());
        assertEquals("person", detachedEdge.iterators().vertexIterator(Direction.IN).next().label());

        assertEquals(0, StreamFactory.stream(detachedEdge.iterators().propertyIterator()).count());
    }

    @Test
    @LoadGraphWith(GraphData.MODERN)
    public void shouldEvaluateToEqual() {
        assertTrue(DetachedEdge.detach(g.e(convertToEdgeId("josh", "created", "lop")).next()).equals(DetachedEdge.detach(g.e(convertToEdgeId("josh", "created", "lop")).next())));
    }

    @Test
    @LoadGraphWith(GraphData.MODERN)
    public void shouldHaveSameHashCode() {
        assertEquals(DetachedEdge.detach(g.e(convertToEdgeId("josh", "created", "lop")).next()).hashCode(), DetachedEdge.detach(g.e(convertToEdgeId("josh", "created", "lop")).next()).hashCode());
    }

    @Test
    @LoadGraphWith(GraphData.MODERN)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldNotEvaluateToEqualDifferentId() {
        final Object joshCreatedLopEdgeId = convertToEdgeId("josh", "created", "lop");
        final Vertex vOut = g.v(convertToVertexId("josh")).next();
        final Vertex vIn = g.v(convertToVertexId("lop")).next();
        final Edge e = vOut.addEdge("created", vIn, "weight", 0.4d);
        assertFalse(DetachedEdge.detach(g.e(joshCreatedLopEdgeId).next()).equals(DetachedEdge.detach(e)));
    }

    @Test
    public void shouldConstructDetachedEdgeFromParts() {
        final Map<String, Object> properties = new HashMap<>();
        properties.put("x", "a");
        properties.put(Graph.Key.hide("y"), "b");

        final DetachedEdge de = new DetachedEdge(10, "bought", properties, Pair.with(1, "person"), Pair.with(2, "product"));

        assertEquals(10, de.id());
        assertEquals("bought", de.label());
        assertEquals("person", de.iterators().vertexIterator(Direction.OUT).next().label());
        assertEquals(1, de.iterators().vertexIterator(Direction.OUT).next().id());
        assertEquals("product", de.iterators().vertexIterator(Direction.IN).next().label());
        assertEquals(2, de.iterators().vertexIterator(Direction.IN).next().id());

        assertEquals(1, StreamFactory.stream(de.iterators()).count());
        assertEquals("a", de.iterators().propertyIterator("x").next().value());
        assertEquals(1, StreamFactory.stream(de.iterators().propertyIterator("x")).count());

        assertEquals("a", de.property("x").value());
        assertEquals("x", de.property("x").key());
        assertFalse(de.property("x").isHidden());

        assertEquals("b", de.property(Graph.Key.hide("y")).value());
        assertEquals(Graph.Key.hide("y"), de.property(Graph.Key.hide("y")).key());
        assertTrue(de.property(Graph.Key.hide("y")).isHidden());
    }

    @Test(expected = UnsupportedOperationException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldNotAllowSetProperty() {
        final Vertex v = g.addVertex();
        final Edge e = v.addEdge("test", v);
        final DetachedEdge detachedEdge = DetachedEdge.detach(e);
        detachedEdge.property("test", "test");
    }

    @Test(expected = UnsupportedOperationException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldNotAllowRemove() {
        final Vertex v = g.addVertex();
        final Edge e = v.addEdge("test", v);
        final DetachedEdge detachedEdge = DetachedEdge.detach(e);
        detachedEdge.remove();
    }

    @Test(expected = UnsupportedOperationException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldNotTraverse() {
        final Vertex v = g.addVertex();
        final Edge e = v.addEdge("test", v);
        final DetachedEdge detachedEdge = DetachedEdge.detach(e);
        detachedEdge.start();
    }
}
