/**
 *  Copyright 2017 DREAM:Lab, Indian Institute of Science, Bangalore
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may
 *  not use this file except in compliance with the License. You may obtain
 *  a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  @author Himanshu Sharma
 *  @author Diptanshu Kakwani
 */

package in.dream_lab.goffish.hama;

import java.util.*;

import in.dream_lab.goffish.api.*;
import org.apache.hadoop.io.Writable;

public class BiSubgraph<S extends Writable, V extends Writable, E extends Writable, I extends Writable, J extends Writable, K extends Writable>
        implements IBiSubgraph<S, V, E, I, J, K> {

    K subgraphID;
    private Map<I, IBiVertex<V, E, I, J, K>> _localVertexMap;
    private HashMap<I, IBiRemoteVertex<V, E, I, J, K>> _remoteVertexMap;
    int partitionID;
    S _value;

    BiSubgraph(int partitionID, K subgraphID) {
        this.partitionID = partitionID;
        this.subgraphID = subgraphID;
        _localVertexMap = new HashMap<I, IBiVertex<V, E, I, J, K>>();
        _remoteVertexMap = new HashMap<I, IBiRemoteVertex<V, E, I, J, K>>();
    }
    @Override
    public void addVertex(IBiVertex<V, E, I, J, K> v) {
        if (v instanceof IBiRemoteVertex)
            _remoteVertexMap.put(v.getVertexId(), (IBiRemoteVertex<V, E, I, J, K>) v);
        else
            _localVertexMap.put(v.getVertexId(), v);
    }

    @Override
    public IBiVertex<V, E, I, J, K> getVertexById(I vertexID) {
        return (_localVertexMap.get(vertexID) == null) ?  _remoteVertexMap.get(vertexID) :
                _localVertexMap.get(vertexID);
    }

    @Override
    public K getSubgraphId() {
        return subgraphID;
    }

    @Override
    public long getVertexCount() {
        return _localVertexMap.size() + _remoteVertexMap.size();
    }

    @Override
    public long getLocalVertexCount() {
        return _localVertexMap.size();
    }

    @Override
    public Iterable<IBiVertex<V, E, I, J, K>> getVertices() {
        return new Iterable<IBiVertex<V, E, I, J, K>>() {

            private Iterator<IBiVertex<V, E, I, J, K>> localVertexIterator = _localVertexMap.values().iterator();
            private Iterator<IBiRemoteVertex<V, E, I, J, K>> remoteVertexIterator = _remoteVertexMap.values().iterator();

            @Override
            public Iterator<IBiVertex<V, E, I, J, K>> iterator() {
                return new Iterator<IBiVertex<V, E, I, J, K>>() {
                    @Override
                    public boolean hasNext() {
                        if (localVertexIterator.hasNext()) {
                            return true;
                        } else {
                            return remoteVertexIterator.hasNext();
                        }
                    }

                    @Override
                    public IBiVertex<V, E, I, J, K> next() {
                        if (localVertexIterator.hasNext()) {
                            return localVertexIterator.next();
                        } else {
                            return (IBiVertex<V, E, I, J, K>) remoteVertexIterator.next();
                        }
                    }

                    @Override
                    public void remove() {

                    }
                };
            }
        };
    }

    @Override
    public Iterable<IBiVertex<V, E, I, J, K>> getLocalVertices() {
        return _localVertexMap.values();
    }

    @Override
    public void setSubgraphValue(S value) {
        _value = value;
    }

    @Override
    public S getSubgraphValue() {
        return _value;
    }


    @Override
    @SuppressWarnings("unchecked")
    public Iterable<IBiRemoteVertex<V, E, I, J, K>> getRemoteVertices() {
        return _remoteVertexMap.values();
    }

    @Override
    public IBiEdge<E, I, J, K> getEdgeById(J edgeID) {
        for (IBiVertex<V, E, I, J, K> vertex : _localVertexMap.values()) {
            for (IBiEdge<E, I, J, K> vertexEdge : vertex.getOutEdges()) {
                if (edgeID.equals(vertexEdge)) {
                    return vertexEdge;
                }
            }
        }
        return null;
    }

    @Override
    public Iterable<IBiEdge<E, I, J, K>> getOutEdges() {
        List<IBiEdge<E, I, J, K>> edgeList = new ArrayList<IBiEdge<E, I, J, K>>();
        for (IBiVertex<V, E, I, J, K> vertex : _localVertexMap.values()) {
            for (IBiEdge<E, I, J, K> vertexEdge : vertex.getOutEdges()) {
                edgeList.add(vertexEdge);
            }
        }
        return edgeList;
    }
}
