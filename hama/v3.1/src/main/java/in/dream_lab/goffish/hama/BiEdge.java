package in.dream_lab.goffish.hama;

import in.dream_lab.goffish.api.IBiEdge;
import org.apache.hadoop.io.Writable;

/**
 * Created by vis on 28/6/17.
 */
public class BiEdge<E extends Writable, I extends Writable, J extends Writable, K extends Writable> implements IBiEdge<E, I, J, K> {

    private K _source;
    E _value;
    J edgeID;
    I _sink;

    BiEdge(J id, I sinkID) {
        edgeID = id;
        _sink = sinkID;
    }

    BiEdge(K sourceID, J id, I sinkID) {
        this(id, sinkID);
        _source = sourceID;
    }

    void setSinkID(I sinkID) {
        _sink = sinkID;
    }

    @Override
    public E getValue() {
        return _value;
    }

    @Override
    public void setValue(E val) {
        _value = val;
    }

    @Override
    public J getEdgeId() {
        return edgeID;
    }

    @Override
    public I getSinkVertexId() {
        return _sink;
    }

    @Override
    public void setSourceID(K sourceID) {
        _source = sourceID;
    }

    @Override
    public K getSourceVertexId() {
        return _source;
    }





}
