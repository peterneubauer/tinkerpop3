package com.tinkerpop.gremlin.process.graph.step.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.Barrier;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.TraverserSet;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StallStep<S> extends AbstractStep<S, S> implements Barrier {
    private final TraverserSet<S> traverserSet = new TraverserSet<>();
    private final long stallTime; // 1,000,000 nanoseconds in a millisecond

    public StallStep(final Traversal traversal, final long stallTime) {
        super(traversal);
        this.stallTime = stallTime * 1000000l;
    }

    @Override
    public Traverser<S> processNextStart() {
        if (this.starts.hasNext()) {
            final long currentTime = System.nanoTime();
            while (this.starts.hasNext() && (System.nanoTime() - currentTime) < this.stallTime) {
                this.traverserSet.add(this.starts.next());
            }
            return this.traverserSet.remove();
        } else {
            return this.traverserSet.remove();
        }
    }

    @Override
    public void reset() {
        super.reset();
        this.traverserSet.clear();
    }
}
