package in.dream_lab.goffish.giraph.graph;

import in.dream_lab.goffish.api.IRemoteVertex;
import in.dream_lab.goffish.api.ISubgraph;
import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IVertex;
import org.apache.giraph.graph.*;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.util.HashMap;
import java.util.Iterator;

/**
 * Created by anirudh on 27/09/16.
 *
 * @param <S>  Subgraph id
 * @param <I>  Vertex id
 * @param <V>  Vertex value
 * @param <E>  Edge data
 * @param <SV> Subgraph Value type
 */
public class DefaultSubgraph<SV extends Writable, V extends Writable, E extends Writable, I extends WritableComparable, EI extends WritableComparable, S extends WritableComparable
    >
    extends DefaultVertex<SubgraphId<S>, SubgraphVertices<SV, V, E, I, EI, S>, E> implements ISubgraph<SV, V, E, I, EI, S> {

  public void setRemoteVertices(HashMap<I, IRemoteVertex<V, E, I, EI, S>> remoteVertices) {
    SubgraphVertices<SV, V, E, I, EI, S> subgraphVertices = getValue();
    subgraphVertices.setRemoteVertices(remoteVertices);
  }

  public Iterable<IRemoteVertex<V, E, I, EI, S>> getRemoteVertices() {
    SubgraphVertices<SV, V, E, I, EI, S> subgraphVertices = getValue();
    return subgraphVertices.getRemoteVertices().values();
  }

  @Override
  public IEdge<E, EI, I, Writable> getEdgeById(EI edgeId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setSubgraphValue(SV value) {
    getSubgraphVertices().setSubgraphValue(value);
  }

  @Override
  public SV getSubgraphValue() {
    return getSubgraphVertices().getSubgraphValue();
  }

  @Override
  public Iterable<S> getRemoteSubgraphsID() {
    return null;
  }

  @Override
  public Iterable<IEdge> getRemoteInEdges() {
    return null;
  }


  public SubgraphVertices<SV, V, E, I, EI, S> getSubgraphVertices() {
    return getValue();
  }

  @Override
  public IVertex<V, E, I, EI> getVertexById(I vertexId) {
    return getSubgraphVertices().getVertexById(vertexId);
  }

  @Override
  public S getSubgraphId() {
    return getId().getSubgraphId();
  }

  @Override
  public long getVertexCount() {
    return getSubgraphVertices().getNumVertices() + getSubgraphVertices().getNumRemoteVertices();
  }

  @Override
  public long getLocalVertexCount() {
    return getSubgraphVertices().getNumVertices();
  }

  @Override
  public Iterable<IVertex<V, E, I, EI>> getVertices() {
    return getSubgraphVertices().getVertices();
  }

  @Override
  public Iterable<IVertex<V, E, I, EI>> getLocalVertices() {
    return getSubgraphVertices().getLocalVertices().values();
  }

  public int getPartitionId() {
    return getId().getPartitionId();
  }

  public Iterable<IEdge<E, EI, I, Writable>> getOutEdges() {
    return new Iterable<IEdge<E, EI, I, Writable>>() {
      @Override
      public Iterator<IEdge<E, EI, I, Writable>> iterator() {
        return new EdgeIterator();
      }
    };
  }

  private final class EdgeIterator implements Iterator<IEdge<E, EI, I, Writable>> {
    Iterator<IVertex<V, E, I, EI>> vertexMapIterator;
    Iterator<IEdge<E, EI, I, Writable>> edgeIterator;

    public EdgeIterator() {
      vertexMapIterator = getVertices().iterator();
      IVertex<V, E, I, EI> nextVertex = vertexMapIterator.next();
      edgeIterator = nextVertex.getOutEdges().iterator();
    }

    @Override
    public boolean hasNext() {
      if (edgeIterator.hasNext()) {
        return true;
      } else {
        while (vertexMapIterator.hasNext()) {
          IVertex<V, E, I, EI> nextVertex = vertexMapIterator.next();
          edgeIterator = nextVertex.getOutEdges().iterator();
          if (edgeIterator.hasNext()) {
            return true;
          }
        }
      }
      return false;
    }

    public IEdge<E, EI, I, Writable> next() {
      if (edgeIterator.hasNext()) {
        return edgeIterator.next();
      } else {
        while (vertexMapIterator.hasNext()) {
          IVertex<V, E, I, EI> nextVertex = vertexMapIterator.next();
          edgeIterator = nextVertex.getOutEdges().iterator();
          if (edgeIterator.hasNext()) {
            return edgeIterator.next();
          }
        }
      }
      return null;
    }

    // TODO: Raise exception on call to remove
    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

  }



}