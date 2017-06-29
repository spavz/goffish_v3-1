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

import com.google.common.primitives.Longs;
import in.dream_lab.goffish.api.*;
import in.dream_lab.goffish.hama.api.IBiReader;
import in.dream_lab.goffish.hama.api.IControlMessage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.commons.util.KeyValuePair;
import org.apache.hama.util.ReflectionUtils;

import java.io.IOException;
import java.util.*;

/**
 *
 * @author humus
 *
 * @param <S>
 * @param <V>
 * @param <E>
 * @param <K>
 * @param <M>
 *
 * Expected format :
 * pid vid sgid sinkid1 sgid1 pid1 sinkid2 sgid2 pid2 ...
 *
 * VertexId - LongWritable
 * EdgeId - LongWritable
 * SubgraphId - LongWritable
 *
 * As this reader takes split files it might also get those vertices that belong to other partitions
 * superstep 1 - shuffles the vertices around and send them to their respective partition(creates a few obj also)
 * superstep 2 - create objects(vertex,edge and remote vertex). send the subgraphIDs that we have to all other partitions.
 * Superstep 3 - generate subgraphPartitionMapping from the incoming msgs.
 */
public class FullInfoSplitReaderAlpha<S extends Writable, V extends Writable, E extends Writable, K extends Writable, M extends Writable>
    implements
        IBiReader<Writable, Writable, Writable, Writable, S, V, E, LongWritable, LongWritable, LongWritable> {

  public static final Log LOG = LogFactory.getLog(FullInfoSplitReaderAlpha.class);

  private HamaConfiguration conf;
  private BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer;
  private BiPartition<S, V, E, LongWritable, LongWritable, LongWritable> partition;
  private Map<K, Integer> subgraphPartitionMap;
  private Map<LongWritable, ArrayList<IBiEdge<E, LongWritable, LongWritable, LongWritable>>> localinEdgeMap;
  private Map<LongWritable, LongWritable> vertexSubgraphMap;

    private int edgeCount = 0;

  public FullInfoSplitReaderAlpha(
      BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer,
      Map<K, Integer> subgraphPartitionMap) {
    this.peer = peer;
    this.subgraphPartitionMap = subgraphPartitionMap;
    this.conf = peer.getConfiguration();
    partition = new BiPartition<>(peer.getPeerIndex());
    localinEdgeMap = new HashMap<>();
    vertexSubgraphMap = new HashMap<>();
  }

  @Override
  public List<IBiSubgraph<S, V, E, LongWritable, LongWritable, LongWritable>> getSubgraphs()
      throws IOException, SyncException, InterruptedException {

    KeyValuePair<Writable, Writable> pair;
    while ((pair = peer.readNext()) != null) {
      String stringInput = pair.getValue().toString();
      // pid is the first column and its range is 0 to max pid
      int partitionID = Integer
          .parseInt(stringInput.substring(0, stringInput.indexOf('\t')));
      LOG.debug("partitionID = "+partitionID);

      if (partitionID != peer.getPeerIndex()) {
        // send vertex to its correct partition
        Message<K, M> msg = new Message<>();
        msg.setMessageType(Message.MessageType.VERTEX);
        ControlMessage ctrl = new ControlMessage();
        ctrl.setTransmissionType(IControlMessage.TransmissionType.VERTEX);
        ctrl.addextraInfo(stringInput.getBytes());
        msg.setControlInfo(ctrl);
        peer.send(peer.getPeerName(partitionID), msg);

      } else {

        // belongs to this partition
        createVertex(stringInput);
      }
    }


    peer.sync();

    Message<K, M> msg;
    //recieve all incoming vertices
    while ((msg = peer.getCurrentMessage()) != null) {
      ControlMessage receivedCtrl = (ControlMessage) msg.getControlInfo();
      createVertex(new String(
          receivedCtrl.getExtraInfo().iterator().next().copyBytes()));
    }

    for (LongWritable v : localinEdgeMap.keySet())
        for(IBiEdge<E,LongWritable,LongWritable,LongWritable> e: localinEdgeMap.get(v)) {
            BiPartition<S, V, E, LongWritable, LongWritable, LongWritable> p =  partition;
            LongWritable sgid = vertexSubgraphMap.get(v);
            IBiSubgraph<S, V, E, LongWritable, LongWritable, LongWritable> s = p.getSubgraph(sgid);
            IBiVertex<V, E, LongWritable, LongWritable,LongWritable> vertex = s.getVertexById(v);
            vertex.addInEdge(e);

            //partition.getSubgraph(vertexSubgraphMap.get(v)).getVertexById(v).addInEdge(e);
        }


      // broadcast all subgraphs belonging to this partition
    Message<K, M> subgraphMapppingMessage = new Message<>();
    subgraphMapppingMessage.setMessageType(Message.MessageType.CUSTOM_MESSAGE);
    ControlMessage controlInfo = new ControlMessage();
    controlInfo.setTransmissionType(IControlMessage.TransmissionType.BROADCAST);
    controlInfo.setPartitionID(peer.getPeerIndex());
    subgraphMapppingMessage.setControlInfo(controlInfo);
    for (IBiSubgraph<S, V, E, LongWritable, LongWritable, LongWritable> subgraph : partition
        .getSubgraphs()) {

      byte subgraphIDbytes[] = Longs
          .toByteArray(subgraph.getSubgraphId().get());
      controlInfo.addextraInfo(subgraphIDbytes);
    }

    sendToAllPartitions(subgraphMapppingMessage);

    peer.sync();
    Message<K, M> subgraphMappingInfoMessage;
    while ((subgraphMappingInfoMessage = peer.getCurrentMessage()) != null) {
      ControlMessage receivedCtrl = (ControlMessage) subgraphMappingInfoMessage.getControlInfo();
      Integer partitionID = receivedCtrl.getPartitionID();
      for (BytesWritable rawSubgraphID : receivedCtrl.getExtraInfo()) {
        LongWritable subgraphID = new LongWritable(
            Longs.fromByteArray(rawSubgraphID.copyBytes()));
        subgraphPartitionMap.put((K) subgraphID, partitionID);
      }
    }

    return partition.getSubgraphs();
  }

  private void sendToAllPartitions(Message<K, M> message) throws IOException {
    for (String peerName : peer.getAllPeerNames()) {
      peer.send(peerName, message);
    }
  }

  private void createVertex(String stringInput) {

      // belongs to this partition
      String vertexValue[] = stringInput.split("\\s+");

      LongWritable vertexID = new LongWritable(
              Long.parseLong(vertexValue[1]));
      int partitionID = Integer.parseInt(vertexValue[0]);
      LongWritable vertexSubgraphID = new LongWritable(
              Long.parseLong(vertexValue[2]));

      BiSubgraph<S, V, E, LongWritable, LongWritable, LongWritable> subgraph = (BiSubgraph<S, V, E, LongWritable, LongWritable, LongWritable>) partition
              .getSubgraph(vertexSubgraphID);

      if (subgraph == null) {
          subgraph = new BiSubgraph<S, V, E, LongWritable, LongWritable, LongWritable>(
                  partitionID, vertexSubgraphID);
          partition.addSubgraph(subgraph);
      }
      List<IBiEdge<E, LongWritable, LongWritable, LongWritable>> _adjList = new ArrayList<IBiEdge<E, LongWritable, LongWritable, LongWritable>>();

      for (int j = 3; j < vertexValue.length; j++) {
          if (j + 3 > vertexValue.length) {
              LOG.debug("Incorrect length of line for vertex " + vertexID);
          }
          LongWritable sinkID = new LongWritable(Long.parseLong(vertexValue[j]));
          LongWritable sinkSubgraphID = new LongWritable(
                  Long.parseLong(vertexValue[j + 1]));
          int sinkPartitionID = Integer.parseInt(vertexValue[j + 2]);
          j += 2;
          LongWritable edgeID = new LongWritable(
                  edgeCount++ | (((long) peer.getPeerIndex()) << 32));
          IBiEdge<E, LongWritable, LongWritable, LongWritable> e = new BiEdge<E, LongWritable, LongWritable, LongWritable>(vertexID, edgeID, sinkID);
          _adjList.add(e);
          if (sinkPartitionID != peer.getPeerIndex() && subgraph.getVertexById(sinkID) == null) {
              // this is a remote vertex
              IBiRemoteVertex<V, E, LongWritable, LongWritable, LongWritable> sink = new BiRemoteVertex<>(
                      sinkID,  sinkSubgraphID);
              // Add it to the same subgraph, as this is part of weakly connected
              // component
              subgraph.addVertex(sink);
          } else {
              if (localinEdgeMap.get(sinkID) == null) {
                  ArrayList<IBiEdge<E, LongWritable, LongWritable, LongWritable>> a = new ArrayList<>();
                  a.add(e);
                  localinEdgeMap.put(sinkID,a);
              }
              else
                  localinEdgeMap.get(sinkID).add(e);
              vertexSubgraphMap.put(sinkID,sinkSubgraphID);
          }
          subgraph.addVertex(createBiVertexInstance(vertexID, _adjList));
      }
  }

  private IBiVertex<V, E, LongWritable, LongWritable,LongWritable> createVertexInstance(LongWritable vertexID, List<IBiEdge<E, LongWritable, LongWritable, LongWritable>> adjList) {
    return (IBiVertex<V, E, LongWritable, LongWritable, LongWritable>) ReflectionUtils.newInstance(GraphJobRunner.VERTEX_CLASS, new Class<?>[] {Writable.class, Iterable.class},
            new Object[] {vertexID, adjList});
  }

  private IBiVertex<V, E, LongWritable, LongWritable,LongWritable> createBiVertexInstance(LongWritable vertexID, List<IBiEdge<E, LongWritable, LongWritable, LongWritable>> adjList) {
        return ReflectionUtils.newInstance(BiGraphJobRunner.BIVERTEX_CLASS, new Class<?>[] {Writable.class, Iterable.class},
                new Object[] {vertexID, adjList});
  }
}
