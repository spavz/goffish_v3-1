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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.io.Writable;

import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IRemoteVertex;
import in.dream_lab.goffish.api.IVertex;

public class RemoteVertex<V extends Writable, E extends Writable, I extends Writable, J extends Writable, K extends Writable>
    implements IRemoteVertex<V, E, I, J, K> {

  I vertexID;
  K subgraphID;
  V _value;
  private List<IEdge<E, J, I, Writable>> _inadjList;

  public RemoteVertex(I vertexID, K subgraphID) {
    this.vertexID = vertexID;
    this.subgraphID = subgraphID;
    _inadjList = new ArrayList<IEdge<E, J, I, Writable>>();

  }

  public RemoteVertex(I vertexID) {
    this.vertexID = vertexID;
    _inadjList = new ArrayList<IEdge<E, J, I, Writable>>();

  }

  public void setSubgraphID(K subgraphID) {
    this.subgraphID = subgraphID;
  }

  @Override
  public boolean isRemote() {
    return true;
  }

  @Override
  public Collection<IEdge<E, J, I, Writable>> getOutEdges() {
    return null;
  }

  @Override
  public Iterable<IEdge<E, J, I, Writable>> getInEdges() {
    return _inadjList;
  }

  @Override
  public I getVertexId() {
    return vertexID;
  }

  @Override
  public V getValue() {
    throw new NotImplementedException("Remote Vertex does not have a value");
  }

  @Override
  public void setValue(V value) {
    throw new NotImplementedException("Remote Vertex does not have a value");
  }

  @Override
  public IEdge<E, J, I, Writable> getOutEdge(I i) {
    throw new NotImplementedException("Remote Vertex does not have edges");
  }

  @Override
  public void addInEdge(IEdge<E, J, I, Writable> e) {
    _inadjList.add(e);
  }

  @Override
  public K getSubgraphId() {
    return subgraphID;
  }

  @Override
  public void setLocalState(V value) {
    this._value = value;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public boolean equals(Object o) {
    return (this.vertexID).equals(((IVertex) o).getVertexId());
  }

}
