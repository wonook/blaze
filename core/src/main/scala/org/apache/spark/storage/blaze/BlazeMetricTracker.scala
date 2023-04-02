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

import java.util.concurrent.ConcurrentHashMap
import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId

import scala.collection.mutable
import scala.collection.convert.decorateAsScala._

case class DiskBlockMetadata(serializedSize: Long, spillTime: Long)

private[spark] class BlazeMetricTracker extends Logging {

  // Blocks evicted to DiskStore
  val executorIdToDiskBlockIdsMap = new ConcurrentHashMap[String, mutable.Set[BlockId]]()
  val diskBlockIdToMetadataMap = new ConcurrentHashMap[BlockId, DiskBlockMetadata]()

  // Blocks cached in MemoryStore
  val executorIdToMemBlockIdsMap = new ConcurrentHashMap[String, mutable.Set[BlockId]]()
  val memBlockIdToSizeMap = new ConcurrentHashMap[BlockId, Long]()

  // Data to compute recomp time
  val parentBlockIdToBlockIdCompTimeMap = new ConcurrentHashMap[String, Long]()

  // For calculating computation cost
  val blockIdToUnrollingTimeMap = new ConcurrentHashMap[BlockId, Long]()
  val blockIdToCompTimeStampMap = new ConcurrentHashMap[BlockId, Long]()
  val stageIdToSubmissionTimeMap = new mutable.HashMap[Int, Long]()
  val memBlockIdToCompCostMap = new ConcurrentHashMap[BlockId, Long]()

  /**
   * Access-based reference count of each cached block
   * When a block is cached: its parent blocks' refcnt gets -1
   * When a block is evicted: its parent blocks' refcnt gets +1
   * refcnt info is maintained after a block is evicted.
   */
  val blockIdToRefCntMap = new ConcurrentHashMap[BlockId, Int]()
  val blockIdToAccessCntMap = new ConcurrentHashMap[BlockId, Int]()
  val blockIdToTimeStampMap = new ConcurrentHashMap[BlockId, Long]()

  val taskStartTime = new ConcurrentHashMap[String, Long]().asScala

  // for computing refCnt and refDist
  // @see stageSubmitted
  val jobIdToStageIdsMap = new mutable.HashMap[Int, mutable.HashSet[Int]]()

  def isInMemOrDisk(blockId: BlockId): Boolean = {
    executorIdToMemBlockIdsMap.entrySet().iterator().asScala
      .foreach {
        entry => if (entry.getValue.contains(blockId)) {
          return true
        }
      }
    executorIdToDiskBlockIdsMap.entrySet().iterator().asScala
      .foreach {
        entry => if (entry.getValue.contains(blockId)) {
          return true
        }
      }

    false
  }

  // TODO: among serialized and deserialized size of this block, which size to use?
  //  Maybe refactor this into getDeserializedBlockSize and getSerializedBlockSize?
  def getBlockSize(blockId: BlockId): Long = synchronized {
    if (memBlockIdToSizeMap.containsKey(blockId)) {
      memBlockIdToSizeMap.get(blockId)
    } else if (diskBlockIdToMetadataMap.containsKey(blockId)) {
      // TODO: currently we're mixing de/serialized size
      val metadata = diskBlockIdToMetadataMap.get(blockId)
      metadata.serializedSize
    } else -1L
  }

  def getExecutorIdToDiskBlockIdsMap: Map[String, mutable.Set[BlockId]] = {
    executorIdToDiskBlockIdsMap.asScala.toMap
  }

  def getExecutorIdToMemBlockIdsMap: Map[String, mutable.Set[BlockId]] = {
    executorIdToMemBlockIdsMap.asScala.toMap
  }

  // TODO: fault tolerance logic
  def clearAllMemBlocksFromExecutor(executorId: String): Unit = {
    var aggregateSize = 0L
    executorIdToMemBlockIdsMap.remove(executorId).foreach {
      blockId => aggregateSize += memBlockIdToSizeMap.remove(blockId)
    }
    BlazeLogger.clearAllMemBlocks(executorId, aggregateSize)
  }

  def removeBlockFromExecutor(blockId: BlockId, executorId: String, onDisk: Boolean): Unit = {
    if (onDisk) {
      if (diskBlockIdToMetadataMap.containsKey(blockId)) {
        diskBlockIdToMetadataMap.remove(blockId)
      }
      executorIdToDiskBlockIdsMap.get(executorId).remove(blockId)
    } else {
      if (memBlockIdToSizeMap.containsKey(blockId)) {
        memBlockIdToSizeMap.remove(blockId)
      }
      executorIdToMemBlockIdsMap.get(executorId).remove(blockId)
    }
  }

  def registerLocationAndMetadataOfDiskBlock(blockId: BlockId, executorId: String,
                                             serializedSize: Long, spillTime: Long): Unit = {

    if (diskBlockIdToMetadataMap.containsKey(blockId)
    && executorIdToDiskBlockIdsMap.containsKey(executorId)) {
      logWarning("[BLAZE] Block metadata already registered for this executor! "
      + "Maybe re-spilling an already spilled block?")
    }

    diskBlockIdToMetadataMap.put(blockId, DiskBlockMetadata(serializedSize, spillTime))
    executorIdToDiskBlockIdsMap.putIfAbsent(executorId,
      ConcurrentHashMap.newKeySet[BlockId].asScala)
    executorIdToDiskBlockIdsMap.get(executorId).add(blockId)
  }

  def registerLocationAndSizeOfMemBlock(blockId: BlockId, executorId: String,
                                        deserializedSize: Long): Unit = {
    memBlockIdToSizeMap.put(blockId, deserializedSize)
    executorIdToMemBlockIdsMap.putIfAbsent(executorId,
      ConcurrentHashMap.newKeySet[BlockId].asScala)
    executorIdToMemBlockIdsMap.get(executorId).add(blockId)
  }

  def taskStarted(taskId: String): Unit = {
    logInfo(s"[BLAZE] Handling task ${taskId} started")
    taskStartTime.synchronized {
      taskStartTime.putIfAbsent(taskId, System.currentTimeMillis())
    }
  }

  def taskFinished(taskId: String): Unit = synchronized {
    val tct = System.currentTimeMillis() - taskStartTime.get(taskId).get
    logInfo(s"[BLAZE] Handling task $taskId finished $tct")
  }
}
