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
import org.apache.spark.storage.BlockId

private[spark] object BlazeLogger extends Logging {

  /* Caching */

  def isCached(rddId: Int): Unit = {
    logInfo(s"[BLAZE] IS_CACHED\t$rddId")
  }

  def cachingDecision(costFun: String, blockId: BlockId, executor: String,
                      size: Long, prio: Double, compCost: Double): Unit = {
    if (costFun.contains("GD")) {
      logInfo(s"[BLAZE] DECIDED_TO_CACHE\t$blockId\tExecutor $executor\t" +
        s"$costFun\tOldPrio $prio\tNewPrio $compCost\tDeserSize $size")
    } else {
      logInfo(s"[BLAZE] DECIDED_TO_CACHE\t$blockId\tExecutor $executor\t" +
        s"$costFun\tPrio $prio\tMissPenalty(CompCost) $compCost\tDeserSize $size")
    }
  }

  def cachingFailure(blockId: BlockId, executorId: String,
                     deserializedSizeSoFar: Long): Unit = {
    logInfo(s"[BLAZE] CACHING_FAIL_MEM\tExecutor $executorId\t$blockId\t" +
      s"DeserSizeSoFar $deserializedSizeSoFar")
  }

  def materialized(blockId: BlockId, stageId: Int, executorId: String, size: Long,
                   onDisk: Boolean): Unit = {
    if (onDisk) {
      logInfo(s"[BLAZE] PERSIST_DONE\tStage $stageId\tExecutor $executorId\t$blockId\tSerSize $size")
    } else {
      logInfo(s"[BLAZE] CACHING_DONE\tStage $stageId\tExecutor$executorId\t$blockId\tDeserSize $size")
    }
  }

  def promotionDone(blockId: BlockId, stageId: Int, executorId: String): Unit = {
    logInfo(s"[BLAZE] PROMOTED\t$blockId\tExecutor $executorId\tStage $stageId")
  }


  def discardDecision(blockId: BlockId, executor: String,
                      prio: Double, size: Long, msg: String): Unit = {
    logInfo(s"[BLAZE] DISCARD_MEM\tExecutor $executor\t$blockId\t$prio\t$size\t$msg")
  }


  /* Eviction */

  def evictionFailure(blockId: BlockId, executorId: String): Unit = {
    logInfo(s"[BLAZE] EVICTION_FAIL_MEM\tExecutor $executorId\t$blockId")
  }

  def eviction(blockId: BlockId, costFunction: String,
               executor: String, stageId: Int,
               prio: Double,
               compCost: Double,
               spillTime: Long,
               deserializedSize: Long, serializedSize: Long): Unit = {
    logInfo(s"[BLAZE] EVICTED \t$blockId\t" +
      s"$costFunction Prio $prio\tCompCost $compCost\tSpillTime $spillTime\t" +
      s"Executor $executor\tStage $stageId\t" +
      s"DeserSize $deserializedSize\tSerSize $serializedSize")
  }


  /* Unpersistence */

  def removeMemBlock(stageId: Int, blockId: BlockId, executorId: String): Unit = {
    logInfo(s"[BLAZE] REMOVE_MEM\tStage $stageId\tExecutor $executorId\t$blockId")
  }

  def removeDiskBlock(stageId: Int, blockId: BlockId, executorId: String): Unit = {
    logInfo(s"[BLAZE] REMOVE_DISK\tStage $stageId\tExecutor $executorId\t$blockId")
  }

  /* TODO: fault tolerance? */

  def clearAllMemBlocks(executor: String, aggregateSize: Long): Unit = {
    logInfo(s"[BLAZE] CLEAR_ALL_MEM\tExecutor $executor\tAggregateDeserSize $aggregateSize")
  }


  /* Cost Calculation */

  def unrollingTime(blockId: String, time: Long): Unit = {
    logInfo(s"[BLAZE] UNROLL_TIME\t$blockId\t$time")
  }

  def compTime(key: String, time: Long): Unit = {
    logInfo(s"[BLAZE] COMP_TIME\t$key\t$time")
  }


  /* Just for logging */

  def cacheHit(blockId: BlockId, executorId: String, fromRemote: Boolean,
               onDisk: Boolean, readTime: Long): Unit = {
    if (fromRemote) {
      logInfo(s"[BLAZE] REMOTE_HIT\t$blockId\tExecutor $executorId\t$readTime")
    } else {
      if (onDisk) {
        logInfo(s"[BLAZE] LOCAL_DISK_HIT\t$blockId\tExecutor $executorId\t$readTime")
      } else {
        logInfo(s"[BLAZE] LOCAL_MEM_HIT\t$blockId\tExecutor $executorId\t$readTime")
      }
    }
  }

  def cacheMiss(blockId: BlockId, executorId: String): Unit = {
    logInfo(s"[BLAZE] MISS\t$blockId\tExecutor $executorId")
  }
}
