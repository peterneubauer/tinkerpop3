package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SideEffectCapStrategy extends AbstractTraversalStrategy implements TraversalStrategy {

    private static final SideEffectCapStrategy INSTANCE = new SideEffectCapStrategy();
    private static final Set<Class<? extends TraversalStrategy>> POSTS = Stream.of(LabeledEndStepStrategy.class).collect(Collectors.toSet());

    private SideEffectCapStrategy() {
    }

    @Override
    public void apply(final Traversal<?, ?> traversal, final TraversalEngine engine) {
        if (TraversalHelper.getEnd(traversal) instanceof SideEffectCapable) {
            ((GraphTraversal) traversal).cap();
        }
    }

    @Override
    public Set<Class<? extends TraversalStrategy>> applyPost() {
        return POSTS;
    }

    public static SideEffectCapStrategy instance() {
        return INSTANCE;
    }
}
