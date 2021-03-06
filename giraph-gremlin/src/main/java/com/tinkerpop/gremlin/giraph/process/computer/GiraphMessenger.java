package com.tinkerpop.gremlin.giraph.process.computer;

import com.tinkerpop.gremlin.giraph.process.computer.util.GremlinWritable;
import com.tinkerpop.gremlin.process.computer.MessageType;
import com.tinkerpop.gremlin.process.computer.Messenger;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.apache.hadoop.io.LongWritable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GiraphMessenger<M> implements Messenger<M> {

    private GiraphComputeVertex giraphComputeVertex;
    private Iterable<GremlinWritable> messages;

    public void setCurrentVertex(final GiraphComputeVertex giraphComputeVertex, final Iterable<GremlinWritable> messages) {
        this.giraphComputeVertex = giraphComputeVertex;
        this.messages = messages;
    }

    @Override
    public Iterable<M> receiveMessages(final MessageType messageType) {
        return (Iterable) StreamFactory.iterable(StreamFactory.stream(this.messages).map(m -> m.get()));
    }

    @Override
    public void sendMessage(final MessageType messageType, final M message) {
        if (messageType instanceof MessageType.Local) {
            final MessageType.Local<?, ?> localMessageType = (MessageType.Local) messageType;
            localMessageType.vertices(this.giraphComputeVertex.getBaseVertex()).forEachRemaining(v ->
                    this.giraphComputeVertex.sendMessage(new LongWritable(Long.valueOf(v.id().toString())), new GremlinWritable<>(message)));
        } else {
            final MessageType.Global globalMessageType = (MessageType.Global) messageType;
            globalMessageType.vertices().forEach(v ->
                    this.giraphComputeVertex.sendMessage(new LongWritable(Long.valueOf(v.id().toString())), new GremlinWritable<>(message)));
        }
    }
}
