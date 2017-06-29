package in.dream_lab.goffish.api;

/**
 * Created by vis on 29/6/17.
 */
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

import org.apache.hadoop.io.Writable;

public interface IBiPartition<S extends Writable, V extends Writable, E extends Writable, I extends Writable, J extends Writable, K extends Writable>{

    int getPartitionId();

    void addSubgraph(IBiSubgraph<S, V, E, I, J, K> subgraph);

    void removeSubgraph(K subgraphID);

    //Return in descending order for vertices.
    Iterable<IBiSubgraph<S, V, E, I, J, K>> getSubgraphs();

    IBiSubgraph<S, V, E, I, J, K> getSubgraph(K subgraphID);
}
