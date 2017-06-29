package in.dream_lab.goffish.hama;

import in.dream_lab.goffish.api.IBiEdge;
import in.dream_lab.goffish.api.IBiRemoteVertex;
import in.dream_lab.goffish.api.IBiVertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by vis on 28/6/17.
 */
public class BiRemoteVertex<V extends Writable, E extends Writable, I extends Writable, J extends Writable, K extends Writable> implements IBiRemoteVertex<V, E, I, J, K> {

    private List<IBiEdge<E, I, J, K>> _adjList;
    private List<IBiEdge<E, I, J, K>> _inadjList;
    private I vertexID;
    private V _value;

    BiRemoteVertex(LongWritable sinkID, LongWritable sinkSubgraphID) {
        _adjList = new ArrayList<IBiEdge<E, I, J, K>>();
    }

    BiRemoteVertex(I ID) {
        this();
        vertexID = ID;
    }

    BiRemoteVertex(I Id, Iterable<IBiEdge<E, I, J, K>> edges) {
        this(Id);
        for (IBiEdge<E, I, J, K> e : edges)
            _adjList.add(e);
    }

    public BiRemoteVertex() {

    }

    void setVertexID(I vertexID) {
        this.vertexID = vertexID;
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


    @Override
    public Iterable<IBiEdge<E, I, J, K>> getInEdges() {
        return _inadjList;
    }


    @Override
    public void addInEdge(IBiEdge<E, I, J, K> e) {
        _inadjList.add(e);
    }

    @Override
    public void addInEdges(ArrayList<IBiEdge<E, I, J, K>> edges) {
        for(IBiEdge e: edges)
            _inadjList.add(e);
    }


    @Override
    public K getSubgraphId() {
        return null;
    }

    @Override
    public void setLocalState(V value) {

    }

    @Override
    public V getLocalState() {
        return null;
    }
}
