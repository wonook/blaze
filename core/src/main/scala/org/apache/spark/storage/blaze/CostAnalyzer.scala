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

import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

private[spark] abstract class CostAnalyzer(val costType: String,
                                           val rddJobDag: RDDJobDag,
                                           val metricTracker: BlazeMetricTracker) extends Logging {

  @volatile
  var sortedMemBlockByCompCost: AtomicReference[Map[String,
    mutable.ListBuffer[Cost]]] =
    new AtomicReference[Map[String, mutable.ListBuffer[Cost]]](null)

  def getCostOfCachedBlock(executorId: String, blockId: BlockId): Cost

  /**
   * Sort cached blocks either in MemoryStore or in DiskStore
   * by cost, in ascending order. (smaller one comes earlier)
   *
   * @param executorIdToBlockIdsMap executorIdToMemBlockIdsMap or executorIdToDiskBlockIdsMap
   * @return Map of (executorId, sorted cost list)
   */
  def sortCostOfCachedBlocks(executorIdToBlockIdsMap: Map[String, mutable.Set[BlockId]]):
  Map[String, ListBuffer[Cost]] = {
    executorIdToBlockIdsMap.map(entry => {
      val executorId = entry._1
      val blocks = entry._2
      val costList = blocks.map(blockId => {
        getCostOfCachedBlock(executorId, blockId)
      }).toList
      (executorId, costList.sortWith(_.cost < _.cost).to[ListBuffer])
    })
  }

  def costListToString(costList: ListBuffer[Cost]): String = {
    val sb = new StringBuilder
    costList.map(cost => {
      // computation cost for Blaze,
      // snapshot of computation cost for other policies
      // val calcOnlyCompCost = rddJobDag.calculateCompCost(cost.blockId).toDouble
      s"${cost.blockId}:${cost.cost} "
    })
      .foreach(s => sb.append(s))
    sb.toString()
  }

  /**
   * Periodically update the cost of cached blocks.
   */
  def update: Unit = {
    val st = System.currentTimeMillis()
    sortedMemBlockByCompCost.set(
      sortCostOfCachedBlocks(metricTracker.getExecutorIdToMemBlockIdsMap))
    val updateTime = System.currentTimeMillis() - st

    val sb = new StringBuilder
    sortedMemBlockByCompCost.get()
      .map(entry => s"Executor ${entry._1}: ${costListToString(entry._2)}\n")
      .foreach(s => sb.append(s))

    logInfo(s"------------- Cost map (BlockId:Priority:CompCost) -----------\n" +
      s"${sb.toString()}\n" +
      s"------------- Update time (ms) $updateTime ----------------\n")
  }

  def findZeroUtilityBlocks(): mutable.Set[BlockId] = {
    sortedMemBlockByCompCost.set(
      sortCostOfCachedBlocks(metricTracker.getExecutorIdToMemBlockIdsMap))

    val zeroUtilBlockIds = new mutable.HashSet[BlockId]()
    sortedMemBlockByCompCost.get().values.foreach(costList => {
      costList.foreach(cost => {
        if (cost.cost == 0.0) {
          zeroUtilBlockIds.add(cost.blockId)
        }
      })
    })

    zeroUtilBlockIds
  }
}

/**
 * Cost used for caching and eviction decisions.
 *
 * @param blockId
 * @param cost
 */
class Cost(val blockId: BlockId, var cost: Double)


object CostAnalyzer {
  def apply(costType: String,
            rddJobDag: Option[RDDJobDag],
            metricTracker: BlazeMetricTracker): CostAnalyzer = {

      if (costType.equals("LRU")) {
        new LRUCostAnalyzer(costType, rddJobDag.get, metricTracker)
      } else if (costType.equals("LFU")) {
        new LFUCostAnalyzer(costType, rddJobDag.get, metricTracker)
      } else if (costType.equals("Size")) {
        new SizeCostAnalyzer(costType, rddJobDag.get, metricTracker)
      } else if (costType.equals("LRC")) {
        new LRCCostAnalyzer(costType, rddJobDag.get, metricTracker)
      } else if (costType.equals("MRD")) {
        new MRDCostAnalyzer(costType, rddJobDag.get, metricTracker)
      } else if (costType.equals("GD")) {
        new GDCostAnalyzer(costType, rddJobDag.get, metricTracker)
      } else if (costType.equals("None")) {
        new NullCostAnalyzer(costType, rddJobDag.get, metricTracker)
      } else {
        throw new RuntimeException(s"Unsupported cost function: $costType")
      }
  }
}

