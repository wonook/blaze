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

import org.apache.spark.storage.BlockId

private[spark] object BlazeBlockManagerMessages {

  //////////////////////////////////////////////////////////////////////////////////
  // Messages from executors to the driver.
  //////////////////////////////////////////////////////////////////////////////////
  sealed trait ToBlockManagerMaster

  case class IsCachedRDD(rddId: Int) extends ToBlockManagerMaster

  /* Caching */

  case class CachingDecision(blockId: BlockId, deserSize: Long, executorId: String)
    extends ToBlockManagerMaster

  case class CachingFailure(blockId: BlockId, deserializedSize: Long, executorId: String)
    extends ToBlockManagerMaster

  case class Materialized(blockId: BlockId, stageId: Int, executorId: String,
                          time: Long, size: Long, onDisk: Boolean)
    extends ToBlockManagerMaster

  /* Promotion */

  case class PromotionDecision(blockId: BlockId, executorId: String) extends ToBlockManagerMaster
  case class PromotionDone(blockId: BlockId, stageId: Int, executorId: String)
    extends ToBlockManagerMaster

  /* Eviction */

  case class VictimSelection(incomingBlockId: Option[BlockId], executorId: String,
                             numBytesToFree: Long, stageId: Int)
    extends ToBlockManagerMaster

  case class EvictionFailure(blockId: BlockId, executorId: String)
    extends ToBlockManagerMaster

  case class EvictionDone(blockId: BlockId, executorId: String, stageId: Int)
    extends ToBlockManagerMaster

  /* Cost Calculation */

  // for recomp cost
  case class SendUnrollingTime(blockId: BlockId, time: Long) extends ToBlockManagerMaster
  case class SendCompTime(blockId: BlockId, key: String, time: Long) extends ToBlockManagerMaster

  // size
  case class GetDeserializedSize(blockId: BlockId, executorId: String) extends ToBlockManagerMaster
  case class GetSerializedSize(blockId: BlockId, executorId: String) extends ToBlockManagerMaster

  /* Just for logging */

  case class CacheHit(blockId: BlockId, executorId: String, fromRemote: Boolean,
                      onDisk: Boolean, readTime: Long) extends ToBlockManagerMaster
  case class CacheMiss(blockId: BlockId, executorId: String) extends ToBlockManagerMaster
}
