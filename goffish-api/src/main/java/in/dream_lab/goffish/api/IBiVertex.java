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
*/

package in.dream_lab.goffish.api;

import org.apache.hadoop.io.Writable;

import java.util.ArrayList;

/* Defines Vertex interface. Could be used to define multiple graph representation,
 * e.g: adjacency list, adjacency set.
 * @param <V> Vertex value object type
 * @param <E> Edge value object type
 * */
public interface IBiVertex<V extends Writable, E extends Writable, I extends Writable, J extends Writable, K extends Writable> {

  I getVertexId();

  boolean isRemote();

  // K getSubgraphID(); Seperate interface

  V getValue();

  void setValue(V value);

  IBiEdge<E, I, J, K> getOutEdge(I vertexId);

  Iterable<IBiEdge<E, I, J, K>> getOutEdges();

  Iterable<IBiEdge<E, I, J, K>> getInEdges();// * addition to IVertex

  void addInEdge(IBiEdge<E, I, J, K> e);

  void addInEdges(ArrayList<IBiEdge<E, I, J, K>> edges);

}
