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

import org.apache.hadoop.io.Writable;

import in.dream_lab.goffish.api.IEdge;

public class Edge<E extends Writable, J extends Writable, I extends Writable, K extends  Writable>
    implements IEdge<E, J, I, Writable> {
  private K _source;
  private E _value;
  private J edgeID;
  private I _sink;

  Edge(J id, I sinkID) {
    edgeID = id;
    _sink = sinkID;
  }

  Edge(K sourceID,J id, I sinkID) {
    edgeID = id;
    _sink = sinkID;
    _source = sourceID;
  }

  void setSinkID(I sinkID) {
    _sink = sinkID;
  }

  void setSourceID(K sourceID) {
    _source = sourceID;
  }

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
  public K getSourceVertexId() {
    return _source;
  }

  //A slight trick to make the vertex
}
