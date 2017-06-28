package in.dream_lab.goffish.api;

import org.apache.hadoop.io.Writable;

/**
 * Created by vis on 28/6/17.
 */
public interface IBiEdge<E extends Writable, I extends Writable, J extends Writable, K extends Writable> {

    J getEdgeId();

    I getSinkVertexId();// add getSinkVertex();

    E getValue();

    void setValue(E value);

    void setSourceID(K sourceID);

    K getSourceVertexId();

}
