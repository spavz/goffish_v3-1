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

import java.io.IOException;

public abstract class IBiAbstractSubgraphComputation<S extends Writable, V extends Writable, E extends Writable, M extends Writable, I extends Writable, J extends Writable, K extends Writable> {

  private IBiSubgraphCompute<S, V, E, M, I, J, K> subgraphPlatformCompute;

  public long getSuperstep() {
    return subgraphPlatformCompute.getSuperstep();
  }

  public void setSubgraphPlatformCompute(IBiSubgraphCompute<S, V, E, M, I, J, K> subgraphPlatformCompute) {
    this.subgraphPlatformCompute = subgraphPlatformCompute;
  }
  
  public IBiSubgraph<S, V, E, I, J, K> getSubgraph() {
    return subgraphPlatformCompute.getSubgraph();
  }

  public void voteToHalt() {
    subgraphPlatformCompute.voteToHalt();
  }
  
  public boolean hasVotedToHalt() {
    return subgraphPlatformCompute.hasVotedToHalt();  
  }

  public abstract void compute(Iterable<IMessage<K, M>> messages) throws IOException;

  public void sendMessage(K subgraphId, M message) {
    subgraphPlatformCompute.sendMessageToSubgraph(subgraphId, message);
  }

  public void sendToNeighbors(M message) {
    subgraphPlatformCompute.sendToNeighbors(message);
  }


  public void sendMessage(K subgraphID, Iterable<M> message) {
    subgraphPlatformCompute.sendMessage(subgraphID, message);
  }


  public void sendToAll(Iterable<M> message) {
    subgraphPlatformCompute.sendToAll(message);
  }
  
  public void sendToAll(M message) {
	subgraphPlatformCompute.sendToAll(message);
  }

  public void sendToNeighbors(Iterable<M> message) {
    subgraphPlatformCompute.sendToNeighbors(message);
  }

  public String getConf(String key) {
    return subgraphPlatformCompute.getConf(key);
  }

}
