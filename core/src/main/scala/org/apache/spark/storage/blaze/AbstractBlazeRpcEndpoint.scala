/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.storage.blaze

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEndpoint
import org.apache.spark.storage.BlockId
import org.apache.spark.Partition

import scala.collection.{mutable}

/**
 */
private[spark] abstract class AbstractBlazeRpcEndpoint extends Logging with RpcEndpoint {

  def isCachedRDD(rddId: Int): Boolean
  def isUserCachedRDD(rddId: Int): Boolean
  def addUserCachedRDD(rddId: Int): Unit
  def addShuffledRDD(rddId: Int): Unit
  def stageSubmitted(stageId: Int, jobId: Int,
                     rdd: String, partitions: Array[Partition], numPartitions: Int): Unit
  def stageCompleted(stageId: Int): Unit
  def taskStarted(taskId: String): Unit
  def taskFinished(taskId: String): Unit

  def removeExecutor(executorId: String): Unit
  def removeBlockFromExecutor(blockId: BlockId, executorId: String, onDisk: Boolean): Unit
  def removeMetadata(rdds: scala.collection.Set[Int]): Unit
  def removeMetadataOfBlocks(stageId: Int, removedBlocks: mutable.Set[BlockId]): Unit
}




