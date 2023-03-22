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
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.storage._
import org.apache.spark.storage.blaze.BlazeBlockManagerMessages._

class BlazeManager(var driverEndpoint: RpcEndpointRef) extends Logging {

  /* Caching */

  // TODO: Current version is inefficient
  //  - requires driver-executor comm for every RDD
  /**
   * For reuse-based autocaching, return true if it is marked as so.
   * For lazy autocaching, return true only when it will not be hidden
   * by later RDDs in the same task.
   * @param rddId
   * @return
   */
  def isCachedRDD(rddId: Int): Boolean = {
    driverEndpoint.askSync[Boolean](IsCachedRDD(rddId))
  }

  /**
   * Invariant to hold:
   * cachingDecision() == true, then the block is always successfully cached in MemoryStore
   * not enough memory || cachingDecision() == false, then we'll retry cachingDecision in DiskStore
   *
   * In MemoryStore, this method is always called with *accurate* deserialized size of the block.
   * TODO: add size estimation by building assumptions in the profiling phase
   * 1) Before unrolling: if we know the size and there won't be enough memory, fail fast
   * 2) During unrolling: if not enough memory, fail
   * 3) After successfully finished unrolling: call cachingDecision() with accurate size info
   *
   * @param blockId to cache in MemoryStore or DiskStore
   * @param deserializedSize we use deserialized size both in MemoryStore and DiskStore
   * @param executorId executor that this block is stored
   * @return whether to actually store this block or not
   */
  def cachingDecision(blockId: BlockId, deserializedSize: Long, executorId: String): Boolean = {
    driverEndpoint.askSync[Boolean](
      CachingDecision(blockId, deserializedSize, executorId))
  }

  /**
   * Before or during unrolling in MemoryStore, we ran out of memory.
   * Log the event, and clear metadata.
   *
   * @param blockId of the block that we failed to cache in MemoryStore
   * @param deserializedSizeSoFar either actual deserialized size,
   *                              or memory used to unroll the block at the point of caching failure
   * @param executorId where this block is stored
   */
  def cachingFailure(blockId: BlockId, deserializedSizeSoFar: Long,
                     executorId: String): Unit = {
    driverEndpoint.ask(CachingFailure(blockId, deserializedSizeSoFar, executorId))
  }

  /**
   * Successfully stored the block in either MemoryStore or DiskStore.
   * Log the event, and register the location and size of it.
   *
   * @param blockId of the stored block
   * @param executorId where this block is stored
   * @param time unroll time of spill time of this block
   * @param size serialized or deserialized size of this block
   * @param onDisk whether we cache the block in DiskStore
   */
  def materialized(blockId: BlockId, stageId: Int, executorId: String,
                   time: Long, size: Long, onDisk: Boolean): Unit = {
    driverEndpoint.ask(Materialized(blockId, stageId, executorId, time, size, onDisk))
  }


  /* Promotion */

  def promotionDecision(blockId: BlockId, executorId: String): Boolean = {
    driverEndpoint.askSync[Boolean](PromotionDecision(blockId, executorId))
  }

  def promotionDone(blockId: BlockId, stageId: Int, executorId: String): Unit = {
    driverEndpoint.ask(PromotionDone(blockId, stageId, executorId))
  }


  /* Eviction */

  /**
   * Select victim blocks s.t. their aggregate size is enough to store blockId.
   *
   * @param incomingBlockId the block we are freeing space for (*not* the victim block!)
   * @param executorId
   * @param numBytesToFree
   * @return list of (victim BlockId, spillToDisk) tuple
   */
  def victimSelection(incomingBlockId: Option[BlockId], executorId: String,
                      numBytesToFree: Long, stageId: Int):
  List[(BlockId, Boolean)] = {
    driverEndpoint.askSync[List[(BlockId, Boolean)]](
      VictimSelection(incomingBlockId, executorId, numBytesToFree, stageId))
  }

  /**
   * We failed to hold lockForWriting, which is required to evict this block.
   * Log the event.
   *
   * @param blockId of the block that we failed to evict
   * @param executorId where this block is stored
   */
  def evictionFailure(blockId: BlockId, executorId: String): Unit = {
    driverEndpoint.ask(EvictionFailure(blockId, executorId))
  }

  /**
   * Successfully dropped the victim block from MemoryStore.
   * Log the event, clear metadata,
   * and record recomputation time, spill time, serialized and deserialized size.
   *
   * @param blockId of the victim block that is dropped
   * @param executorId where this block is stored
   */
  def evictionDone(blockId: BlockId, executorId: String, stageId: Int): Unit = {
    driverEndpoint.ask(EvictionDone(blockId, executorId, stageId))
  }


  /* Cost Calculation */

  // for recomp cost
  def sendUnrollingTime(blockId: BlockId, time: Long): Unit = {
    driverEndpoint.ask(SendUnrollingTime(blockId, time))
  }

  def sendCompTime(blockId: BlockId, key: String, time: Long): Unit = {
    driverEndpoint.ask(SendCompTime(blockId, key, time))
  }

  // size
  def getDeserializedSize(blockId: BlockId, executorId: String): Long = {
    driverEndpoint.askSync[Long](GetDeserializedSize(blockId, executorId))
  }

  def getSerializedSize(blockId: BlockId, executorId: String): Long = {
    driverEndpoint.askSync[Long](GetSerializedSize(blockId, executorId))
  }

  /* Just for logging */

  def cacheHit(blockId: BlockId, executorId: String, fromRemote: Boolean,
               onDisk: Boolean, time: Long): Unit = {
    driverEndpoint.ask(CacheHit(blockId, executorId, fromRemote, onDisk, time))
  }

  def cacheMiss(blockId: BlockId, executorId: String): Unit = {
    driverEndpoint.ask(CacheMiss(blockId, executorId))
  }
}
