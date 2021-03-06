package com.tinkerpop.gremlin.process.graph.step.sideEffect

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex

import static com.tinkerpop.gremlin.structure.Operator.sum

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovySackTest {

    public static class StandardTest extends SackTest {
        @Override
        public Traversal<Vertex, Float> get_g_V_withSackX0_sumX_asXaX_outE_sackXsum_weightX_inV_jumpXa_2X_sack() {
            g.V.withSack({ 0.0f }, sum).as('a').outE.sack(sum, 'weight').inV.jump('a', 2).sack
        }

        @Override
        public Traversal<Vertex, Map> get_g_V_withSackXmap__map_cloneX_out_out_sackXmap_a_nameX_sack() {
            g.V.withSack { [:] } { m -> m.clone() }.out().out().sack { m, v -> m['a'] = v.name; m }.sack()
        }
    }

    public static class ComputerTest extends SackTest {
        @Override
        public Traversal<Vertex, Float> get_g_V_withSackX0_sumX_asXaX_outE_sackXsum_weightX_inV_jumpXa_2X_sack() {
            ComputerTestHelper.compute("g.V.withSack({0.0f},sum).as('a').outE.sack(sum, 'weight').inV.jump('a', 2).sack", g)
        }

        @Override
        public Traversal<Vertex, Map> get_g_V_withSackXmap__map_cloneX_out_out_sackXmap_a_nameX_sack() {
            ComputerTestHelper.compute("g.V.withSack { [:] } { m -> m.clone() }.out().out().sack { m, v -> m['a'] = v.name; m }.sack()", g);
        }
    }
}
