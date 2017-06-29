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

import in.dream_lab.goffish.api.*;
import org.apache.hadoop.io.Writable;

import java.io.IOException;

public class BiSubgraphCompute<S extends Writable, V extends Writable, E extends Writable, M extends Writable, I extends Writable, J extends Writable, K extends Writable>
    implements IBiSubgraphCompute<S, V, E, M, I, J, K> {

  private IBiSubgraph<S, V, E, I, J, K> subgraph;
  private IBiAbstractSubgraphComputation<S, V, E, M, I, J, K> abstractSubgraphCompute;
  long superStepCount;
  boolean voteToHalt;
  private BiGraphJobRunner<S, V, E, M, I, J, K> runner;



  @Override
  public long getSuperstep() {
    return runner.getSuperStepCount();
  }

  @Override
  public IBiSubgraph<S, V, E, I, J, K> getSubgraph() {
    return subgraph;
  }

  @Override
  public void voteToHalt() {
    voteToHalt = true;
  }

  @Override
  public boolean hasVotedToHalt() {
    return voteToHalt;
  }

  /*
   * Resumes the subgraph from halted state
   */
  void setActive() {
    this.voteToHalt = false;
  }
  @Override
  public void setSubgraph(IBiSubgraph<S, V, E, I, J, K> subgraph) {
    this.subgraph = subgraph;
  }


  public void init(BiGraphJobRunner<S, V, E, M, I, J, K> svemijkBiGraphJobRunner) {
    this.runner = svemijkBiGraphJobRunner;
    this.voteToHalt = false;
  }



  @Override
  public void sendMessage(K subgraphID, Iterable<M> messages) {
    for (M message : messages) {
      this.sendMessageToSubgraph(subgraphID, message);
    }
  }

  @Override
  public void sendMessageToSubgraph(K subgraphID, M message) {
    runner.sendMessage(subgraph.getSubgraphId(), subgraphID, message);
  }

  @Override
  public void sendToVertex(I vertexID, M message) {
    runner.sendToVertex(subgraph.getSubgraphId(), vertexID, message);
  }

  @Override
  public void sendToAll(Iterable<M> messages) {
    for (M message : messages) {
      this.sendToAll(message);
    }
  }

  @Override
  public void sendToAll(M message) {
    runner.sendToAll(subgraph.getSubgraphId(), message);
  }

  @Override
  public void sendToNeighbors(Iterable<M> messages) {
    for (M message : messages) {
      this.sendToNeighbors(message);
    }
  }

  @Override
  public void sendToNeighbors(M message) {
    runner.sendToNeighbors(subgraph, message);
  }

  public void setAbstractSubgraphCompute(
          IBiAbstractSubgraphComputation<S, V, E, M, I, J, K> comp) {
    this.abstractSubgraphCompute = comp;
  }






  public IBiAbstractSubgraphComputation<S, V, E, M, I, J, K> getAbstractSubgraphCompute() {
    return this.abstractSubgraphCompute;
  }

  public void compute(Iterable<IMessage<K, M>> messages) throws IOException {
    abstractSubgraphCompute.compute(messages);
  }

  @Override
  public String getConf(String key) {
    // TODO Auto-generated method stub
    return null;
  }

}
