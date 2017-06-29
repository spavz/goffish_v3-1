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

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import in.dream_lab.goffish.api.*;
import in.dream_lab.goffish.hama.Message.MessageType;
import in.dream_lab.goffish.hama.api.IBiReader;
import in.dream_lab.goffish.hama.api.IControlMessage;
import in.dream_lab.goffish.hama.api.IReader;
import in.dream_lab.goffish.hama.utils.DisjointSets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.commons.util.KeyValuePair;
import org.apache.hama.util.ReflectionUtils;

import java.io.IOException;
import java.util.*;

/* Reads graph in the adjacency list format:
 * VID Sink1 Sink2 ...
 */
public class BiLongTextAdjacencyListReader<S extends Writable, V extends Writable, E extends Writable, K extends Writable, M extends Writable>
    implements
        IBiReader<Writable, Writable, Writable, Writable, S, V, E, LongWritable, LongWritable, LongWritable> {

  public static final Log LOG = LogFactory
      .getLog(BiLongTextAdjacencyListReader.class);

  Map<Long, IBiVertex<V, E, LongWritable, LongWritable, LongWritable>> vertexMap;
  Map<Long, IBiRemoteVertex<V, E, LongWritable, LongWritable, LongWritable>> remoteVertexMap;
  BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer;
  private Map<K, Integer> subgraphPartitionMap;
  private Map<LongWritable, LongWritable> vertexSubgraphMap;

  public BiLongTextAdjacencyListReader(
      BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer,
      Map<K, Integer> subgraphPartitionMap) {
    this.peer = peer;
    this.subgraphPartitionMap = subgraphPartitionMap;
    this.vertexSubgraphMap = new HashMap<LongWritable, LongWritable>();
  }

  /*
   * Returns the list of subgraphs belonging to the current partition
   */
  @Override
  public List<IBiSubgraph<S, V, E, LongWritable, LongWritable, LongWritable>> getSubgraphs()
      throws IOException, SyncException, InterruptedException {

    KeyValuePair<Writable, Writable> pair;
    long edgeCount = 0;

    vertexMap = Maps.newHashMap();
    remoteVertexMap = Maps.newHashMap();

    while ((pair = peer.readNext()) != null) {
      String stringInput = pair.getValue().toString();
      String vertexValue[] = stringInput.split("\\s+");

      LongWritable vertexID = new LongWritable(Long.parseLong(vertexValue[0]));
      List<IBiEdge<E, LongWritable, LongWritable, LongWritable>> _adjList = new ArrayList<IBiEdge<E, LongWritable, LongWritable, LongWritable>>();

      for (int j = 1; j < vertexValue.length; j++) {
        LongWritable sinkID = new LongWritable(Long.parseLong(vertexValue[j]));
        LongWritable edgeID = new LongWritable(
            edgeCount++ | (((long) peer.getPeerIndex()) << 32));
        IBiEdge<E, LongWritable, LongWritable, LongWritable> e = new BiEdge<E, LongWritable, LongWritable,LongWritable>(vertexID, edgeID, sinkID);
        _adjList.add(e);
      }

      vertexMap.put(vertexID.get(), createVertexInstance(vertexID, _adjList));

    }

    /* Create remote vertex objects. */
    for (IBiVertex<V, E, LongWritable, LongWritable, LongWritable> vertex : vertexMap
        .values()) {
      for (IBiEdge<E, LongWritable, LongWritable, LongWritable> e : vertex.getOutEdges()) {
        LongWritable sinkID = e.getSinkVertexId();
        if (!vertexMap.containsKey(sinkID.get())) {
          IBiRemoteVertex<V, E, LongWritable, LongWritable, LongWritable> sink = new BiRemoteVertex<>(
              sinkID);
          remoteVertexMap.put(sinkID.get(), sink);
        }
      }
    }

    BiPartition<S, V, E, LongWritable, LongWritable, LongWritable> partition = new BiPartition<>(
        peer.getPeerIndex());

    formSubgraphs(partition);

    /*
     * Ask Remote vertices to send their subgraph IDs. Requires 2 supersteps
     * because the graph is directed
     */
    Message<LongWritable, LongWritable> question = new Message<LongWritable, LongWritable>();
    ControlMessage controlInfo = new ControlMessage();
    controlInfo.setTransmissionType(IControlMessage.TransmissionType.BROADCAST);
    question.setControlInfo(controlInfo);
    /*
     * Message format being sent: partitionID remotevertex1 remotevertex2 ...
     */
    byte partitionIDbytes[] = Ints.toByteArray(peer.getPeerIndex());
    controlInfo.addextraInfo(partitionIDbytes);
    for (IBiVertex<V, E, LongWritable, LongWritable,LongWritable> v : remoteVertexMap
        .values()) {
      byte vertexIDbytes[] = Longs.toByteArray(v.getVertexId().get());
      controlInfo.addextraInfo(vertexIDbytes);
    }
    sendToAllPartitions(question);

    peer.sync();

    Message<LongWritable, LongWritable> msg;
    Map<Integer, List<Message<LongWritable, LongWritable>>> replyMessages = new HashMap<Integer, List<Message<LongWritable, LongWritable>>>();
    // Receiving 1 message per partition
    while ((msg = (Message<LongWritable, LongWritable>) peer
        .getCurrentMessage()) != null) {
      /*
       * Subgraph Partition mapping broadcast Format of received message:
       * partitionID subgraphID1 subgraphID2 ...
       */
      if (msg.getMessageType() == MessageType.SUBGRAPH) {
        Iterable<BytesWritable> subgraphList = ((ControlMessage) msg
            .getControlInfo()).getExtraInfo();

        Integer partitionID = Ints
            .fromByteArray(subgraphList.iterator().next().getBytes());

        for (BytesWritable subgraphListElement : Iterables.skip(subgraphList,
            1)) {
          LongWritable subgraphID = new LongWritable(
              Longs.fromByteArray(subgraphListElement.getBytes()));
          subgraphPartitionMap.put((K) subgraphID, partitionID);
        }
        continue;
      }

      /*
       * receiving query to find subgraph id Remote Vertex
       */
      Iterable<BytesWritable> RemoteVertexQuery = ((ControlMessage) msg
          .getControlInfo()).getExtraInfo();

      /*
       * Reply format : sinkID1 subgraphID1 sinkID2 subgraphID2 ...
       */
      Message<LongWritable, LongWritable> subgraphIDReply = new Message<LongWritable, LongWritable>();
      controlInfo = new ControlMessage();
      controlInfo.setTransmissionType(IControlMessage.TransmissionType.NORMAL);
      subgraphIDReply.setControlInfo(controlInfo);

      Integer sinkPartition = Ints
          .fromByteArray(RemoteVertexQuery.iterator().next().getBytes());
      boolean hasAVertex = false;
      for (BytesWritable remoteVertex : Iterables.skip(RemoteVertexQuery, 1)) {
        LongWritable sinkID = new LongWritable(
            Longs.fromByteArray(remoteVertex.getBytes()));
        LongWritable sinkSubgraphID = vertexSubgraphMap.get(sinkID);
        // In case this partition does not have the vertex
        /*
         * Case 1 : If vertex does not exist Case 2 : If vertex exists but is
         * remote, then its subgraphID is null
         */
        if (sinkSubgraphID == null) {
          continue;
        }
        hasAVertex = true;
        byte sinkIDbytes[] = Longs.toByteArray(sinkID.get());
        controlInfo.addextraInfo(sinkIDbytes);
        byte subgraphIDbytes[] = Longs.toByteArray(sinkSubgraphID.get());
        controlInfo.addextraInfo(subgraphIDbytes);
      }
      if (hasAVertex) {
        peer.send(peer.getPeerName(sinkPartition.intValue()),
            (Message<K, M>) subgraphIDReply);
      }
    }

    peer.sync();

    while ((msg = (Message<LongWritable, LongWritable>) peer
        .getCurrentMessage()) != null) {
      Iterable<BytesWritable> remoteVertexReply = ((ControlMessage) msg
          .getControlInfo()).getExtraInfo();

      Iterator<BytesWritable> queryResponse = remoteVertexReply.iterator();
      while (queryResponse.hasNext()) {
        LongWritable sinkID = new LongWritable(
            Longs.fromByteArray(queryResponse.next().getBytes()));
        LongWritable remoteSubgraphID = new LongWritable(
            Longs.fromByteArray(queryResponse.next().getBytes()));
        RemoteVertex<V, E, LongWritable, LongWritable, LongWritable> sink = (RemoteVertex<V, E, LongWritable, LongWritable, LongWritable>) remoteVertexMap
            .get(sinkID.get());
        assert (sink != null);
        sink.setSubgraphID(remoteSubgraphID);
      }
    }
    return partition.getSubgraphs();
  }

  private IBiVertex<V, E, LongWritable, LongWritable, LongWritable> createVertexInstance(LongWritable vertexID, List<IBiEdge<E, LongWritable, LongWritable, LongWritable>> adjList) {
    return ReflectionUtils.newInstance(BiGraphJobRunner.BIVERTEX_CLASS, new Class<?>[] {Writable.class, Iterable.class},
            new Object[] {vertexID, adjList});
  }

  private void sendToAllPartitions(Message<LongWritable, LongWritable> message)
      throws IOException {
    for (String peerName : peer.getAllPeerNames()) {
      peer.send(peerName, (Message<K, M>) message);
    }
  }

  /* Forms subgraphs by finding (weakly) connected components. */
  void formSubgraphs(
      BiPartition<S, V, E, LongWritable, LongWritable, LongWritable> partition)
      throws IOException {

    long subgraphCount = 0;
    Message<LongWritable, LongWritable> subgraphLocationBroadcast = new Message<LongWritable, LongWritable>();

    subgraphLocationBroadcast.setMessageType(MessageType.SUBGRAPH);
    ControlMessage controlInfo = new ControlMessage();
    controlInfo.setTransmissionType(IControlMessage.TransmissionType.BROADCAST);
    subgraphLocationBroadcast.setControlInfo(controlInfo);

    byte partitionBytes[] = Ints.toByteArray(peer.getPeerIndex());
    controlInfo.addextraInfo(partitionBytes);

    // initialize disjoint set
    DisjointSets<IBiVertex<V, E, LongWritable, LongWritable, LongWritable>> ds = new DisjointSets<IBiVertex<V, E, LongWritable, LongWritable, LongWritable>>(
        vertexMap.size() + remoteVertexMap.size());
    for (IBiVertex<V, E, LongWritable, LongWritable, LongWritable> vertex : vertexMap
        .values()) {
      ds.addSet(vertex);
    }
    for (IBiRemoteVertex<V, E, LongWritable, LongWritable,LongWritable> vertex : remoteVertexMap
        .values()) {
      ds.addSet((IBiRemoteVertex<V, E, LongWritable, LongWritable, LongWritable>) vertex);
    }

    // union edge pairs
    for (IBiVertex<V, E, LongWritable, LongWritable, LongWritable> vertex : vertexMap
        .values()) {
      for (IBiEdge<E, LongWritable, LongWritable, LongWritable> edge : vertex.getOutEdges()) {
        IBiVertex<V, E, LongWritable, LongWritable, LongWritable> sink = vertexMap
            .get(edge.getSinkVertexId().get());
        if (sink == null) {
          sink = (IBiRemoteVertex<V, E, LongWritable, LongWritable, LongWritable>) remoteVertexMap.get(edge.getSinkVertexId().get());
        }
        ds.union(vertex, sink);
      }
    }

    Collection<? extends Collection<IBiVertex<V, E, LongWritable, LongWritable, LongWritable>>> components = ds
        .retrieveSets();

    for (Collection<IBiVertex<V, E, LongWritable, LongWritable, LongWritable>> component : components) {
      LongWritable subgraphID = new LongWritable(
          subgraphCount++ | (((long) partition.getPartitionId()) << 32));
      BiSubgraph<S, V, E, LongWritable, LongWritable, LongWritable> subgraph = new BiSubgraph<S, V, E, LongWritable, LongWritable, LongWritable>(
          peer.getPeerIndex(), subgraphID);

      for (IBiVertex<V, E, LongWritable, LongWritable, LongWritable> vertex : component) {
        subgraph.addVertex(vertex);

        // Dont add remote vertices to the VertexSubgraphMap as remote vertex
        // subgraphID is unknown
        if (!vertex.isRemote()) {
          vertexSubgraphMap.put(vertex.getVertexId(), subgraph.getSubgraphId());
        }
      }

      partition.addSubgraph(subgraph);

      byte subgraphIDbytes[] = Longs.toByteArray(subgraphID.get());
      controlInfo.addextraInfo(subgraphIDbytes);

    }
    sendToAllPartitions(subgraphLocationBroadcast);
  }
}
