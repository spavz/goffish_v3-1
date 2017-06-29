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
import in.dream_lab.goffish.hama.api.IBiReader;
import in.dream_lab.goffish.hama.api.IReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.Partitioner;

import java.io.IOException;

public class BiGraphJob extends BSPJob {

  public final static String SUBGRAPH_COMPUTE_CLASS_ATTR = "in.dream_lab.goffish.bisubgraphcompute.class";
  public final static String SUBGRAPH_CLASS_ATTR = "in.dream_lab.goffish.bisubgraph.class";
  public final static String GRAPH_MESSAGE_CLASS_ATTR = "in.dream_lab.goffish.message.class";
  public final static String VERTEX_ID_CLASS_ATTR = "in.dream_lab.goffish.vertexid.class";
  public final static String VERTEX_VALUE_CLASS_ATTR = "in.dream_lab.goffish.vertexvalue.class";
  public final static String EDGE_ID_CLASS_ATTR = "in.dream_lab.goffish.edgeid.class";
  public final static String EDGE_VALUE_CLASS_ATTR = "in.dream_lab.goffish.edgevalue.class";
  public final static String SUBGRAPH_ID_CLASS_ATTR = "in.dream_lab.goffish.subgraphid.class";
  public final static String SUBGRAPH_VALUE_CLASS_ATTR = "in.dream_lab.goffish.subgraphvalue.class";
  public final static String READER_CLASS_ATTR = "in.dream_lab.goffish.reader.class";
  public final static String VERTEX_CLASS_ATTR = "in.dream_lab.goffish.vertex.class";
  public final static String INITIAL_VALUE = "in.dream_lab.goffish.initialvalue";
  public final static String THREAD_COUNT = "in.dream_lab.goffish.threadcount";

  public static final Log LOG = LogFactory.getLog(BiGraphJob.class);

  public BiGraphJob(HamaConfiguration conf,
                    Class<? extends IBiAbstractSubgraphComputation> exampleClass) throws IOException {

    super(conf);
    conf.setBoolean(Constants.ENABLE_RUNTIME_PARTITIONING, false);
    conf.setBoolean("hama.use.unsafeserialization", true);
    conf.setClass(SUBGRAPH_CLASS_ATTR, BiSubgraph.class, IBiSubgraph.class);
    conf.setClass(SUBGRAPH_COMPUTE_CLASS_ATTR, exampleClass, IBiAbstractSubgraphComputation.class);

    this.setBspClass(BiGraphJobRunner.class);
    // Helps to determine the user's jar to distribute in the cluster.
    this.setJarByClass(exampleClass);
    // setting default values
    this.setVertexIDClass(LongWritable.class);
    this.setEdgeIDClass(LongWritable.class);
    this.setSubgraphIDClass(LongWritable.class);

    // this.setPartitioner(HashPartitioner.class);
  }

  @Override
  public void setPartitioner(
      @SuppressWarnings("rawtypes") Class<? extends Partitioner> theClass) {
    super.setPartitioner(theClass);
  }

  /**
   * Set the Vertex ID class for the job.
   */
  public void setVertexIDClass(Class<? extends Writable> cls)
      throws IllegalStateException {
    conf.setClass(VERTEX_ID_CLASS_ATTR, cls, Writable.class);
  }

  /**
   * Set the Vertex value class for the job.
   */
  public void setVertexValueClass(Class<? extends Writable> cls)
      throws IllegalStateException {
    conf.setClass(VERTEX_VALUE_CLASS_ATTR, cls, Writable.class);
  }

  /**
   * Set the Edge ID class for the job.
   */
  public void setEdgeIDClass(Class<? extends Writable> cls)
      throws IllegalStateException {
    conf.setClass(EDGE_ID_CLASS_ATTR, cls, Writable.class);
  }

  /**
   * Set the Edge value class for the job.
   */
  public void setEdgeValueClass(Class<? extends Writable> cls)
      throws IllegalStateException {
    conf.setClass(EDGE_VALUE_CLASS_ATTR, cls, Writable.class);
  }

  /**
   * Set the Subgraph ID class for the job.
   */
  public void setSubgraphIDClass(Class<? extends Writable> cls)
      throws IllegalStateException {
    conf.setClass(SUBGRAPH_ID_CLASS_ATTR, cls, Writable.class);
  }

  /**
   * Set the Subgraph value class for the job.
   */
  public void setSubgraphValueClass(Class<? extends Writable> cls)
      throws IllegalStateException {
    conf.setClass(SUBGRAPH_VALUE_CLASS_ATTR, cls, Writable.class);
  }

  /**
   * Set the Message Type for the Job.
   * 
   * @param cls
   */
  public void setGraphMessageClass(Class<? extends Writable> cls) {
    conf.setClass(GRAPH_MESSAGE_CLASS_ATTR, cls, Writable.class);
  }

  public void setVertexClass(Class<? extends IVertex> cls) {
    conf.setClass(VERTEX_CLASS_ATTR, cls, IVertex.class);
  }
  /**
   * Use RicherSubgraph Class instead of Subgraph for more features
   */
  public void useRicherSubgraph(boolean use) {
    if (use) {
      conf.setClass(SUBGRAPH_CLASS_ATTR, RicherSubgraph.class, IBiSubgraph.class);
    }
  }

  /**
   * Set the Subgraph class for the job.
   */
  // is this needed? can use exampleclass in constructor to do this
  public void setSubgraphComputeClass(
      Class<? extends SubgraphCompute<? extends Writable, ? extends Writable, ? extends Writable, ? extends Writable, ? extends Writable, ? extends Writable, ? extends Writable>> cls)
      throws IllegalStateException {
    conf.setClass(SUBGRAPH_COMPUTE_CLASS_ATTR, cls, ISubgraphCompute.class);
    setInputKeyClass(cls);
    setInputValueClass(NullWritable.class);
  }

  /**
   * Sets the input reader for parsing the input to vertices.
   */
  public void setInputReaderClass(
      @SuppressWarnings("rawtypes") Class<? extends IBiReader> cls) {
    conf.setClass(READER_CLASS_ATTR, cls, IBiReader.class);
  }

  /**
   * Sets the maximum number of supersteps for the application
   * 
   * @param maxIteration
   */
  public void setMaxIteration(int maxIteration) {
    conf.setInt("hama.graph.max.iteration", maxIteration);
  }
  
  public void setThreadCount(int count) {
    conf.setInt(THREAD_COUNT, count);
  }

  /**
   * Sets the initial value that is passed to the constructor of the application
   * at runtime
   * 
   * @param input
   */
  public void setInitialInput(String input) {
    conf.set(INITIAL_VALUE, input);
  }

  @Override
  public void submit() throws IOException, InterruptedException {
    super.submit();
  }

  @Override
  public boolean waitForCompletion(boolean verbose) throws IOException,
          InterruptedException, ClassNotFoundException {
    if (LOG.isInfoEnabled()) {
      String sgValueClass = conf.get(SUBGRAPH_VALUE_CLASS_ATTR);
      String vertexValueClass = conf.get(VERTEX_VALUE_CLASS_ATTR);
      String edgeValueClass = conf.get(EDGE_VALUE_CLASS_ATTR);
      String messageClass = conf.get(GRAPH_MESSAGE_CLASS_ATTR);
      String vertexIdClass = conf.get(VERTEX_ID_CLASS_ATTR);
      String edgeIdClass = conf.get(EDGE_ID_CLASS_ATTR);
      String sgIdClass = conf.get(SUBGRAPH_ID_CLASS_ATTR);
      String sgComputeClass = conf.get(SUBGRAPH_COMPUTE_CLASS_ATTR);
      String sgImplClass = conf.get(SUBGRAPH_CLASS_ATTR);
      String vertexImplClass = conf.get(VERTEX_CLASS_ATTR);
      String readerClass = conf.get(READER_CLASS_ATTR);
      String initialValue = conf.get(INITIAL_VALUE);

      LOG.info("GOFFISH3.TYPE.SG," + sgValueClass + "," + vertexValueClass + "," + edgeValueClass
               + "," + messageClass + "," + vertexIdClass + "," + edgeIdClass + ","
               + sgIdClass + "," + sgComputeClass + "," + sgImplClass + "," + vertexImplClass
               + "," + readerClass + "," + initialValue);

    }
    return super.waitForCompletion(verbose);
  }

}
