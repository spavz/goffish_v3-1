package in.dream_lab.goffish.hama;

import in.dream_lab.goffish.api.IBiEdge;
import in.dream_lab.goffish.api.IBiVertex;
import in.dream_lab.goffish.api.IBiEdge;
import in.dream_lab.goffish.api.IEdge;
import org.apache.hadoop.io.Writable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by vis on 28/6/17.
 */
public class BiVertex<V extends Writable, E extends Writable, I extends Writable, J extends Writable, K extends Writable> implements IBiVertex<V, E, I, J, K> {

    private List<IBiEdge<E, I, J, K>> _adjList;
    private List<IBiEdge<E, I, J, K>> _inadjList;
    private I vertexID;
    private V _value;

    BiVertex() {
        _adjList = new ArrayList<IBiEdge<E, I, J, K>>();
    }

    BiVertex(I ID) {
        this();
        vertexID = ID;
    }

    BiVertex(I Id, Iterable<IBiEdge<E, I, J, K>> edges) {
        this(Id);
        for (IBiEdge<E, I, J, K> e : edges)
            _adjList.add(e);
    }
    @Override
    public I getVertexId() {
        return vertexID;
    }

    @Override
    public boolean isRemote() {
        return false;
    }

    @Override
    public Iterable<IBiEdge<E, I, J, K>> getOutEdges() {
        return _adjList;
    }

    @Override
    public V getValue() {
        return _value;
    }

    @Override
    public Iterable<IBiEdge<E, I, J, K>> getInEdges() {
        return _inadjList;
    }

    @Override
    public void setValue(V value) {
        _value = value;
    }

    @Override
    public IBiEdge<E, I, J, K> getOutEdge(I vertexId) {
        for (IBiEdge<E, I, J, K> e : _adjList)
            if (e.getSinkVertexId().equals(vertexID))
                return e;
        return null;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public boolean equals(Object o) {
        return (this.vertexID).equals(((IBiVertex) o).getVertexId());
    }


}
