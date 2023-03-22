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

/**
 * GreedyDual cost analyzer.
 * Cost.utility becomes the priority (the H value in the paper) of a cached block,
 * which is the recomputation time to generate the block
 * at the time of the cost calculation.
 * (for GreedyDual-Size it is cost / size)
 * We follow original GreedyDual's implementation for priority calculation,
 * as that in GreedyDual-Size paper (Cao & Irani 1997)
 * requires handling global variable L overflow.
 * @param rddJobDag
 * @param metricTracker
 */
private[spark] class GDCostAnalyzer(costType: String,
                                    rddJobDag: RDDJobDag,
                                    metricTracker: BlazeMetricTracker)
  extends CostAnalyzer(costType, rddJobDag, metricTracker) with Logging {

  /**
   * In GreedyDual-Size algorithm, there are only two cases
   * that item priority is calculated - at the point of 1) insertion and 2) cache hit.
   * Costs are recorded in metricTracker.greedyDualHQueue,
   * and fixed until the next time the block is hit.
   *
   * @param executorId where this block is stored.
   * @param blockId of the cached block.
   * @return
   */
  override def getCostOfCachedBlock(executorId: String, blockId: BlockId): Cost = {
    if (!metricTracker.memBlockIdToCompCostMap.containsKey(blockId)) {
      throw new RuntimeException(s"[getCostOfCachedBlock] entry for $blockId doesn't exist")
    }

    val cost = metricTracker.memBlockIdToCompCostMap.get(blockId)
    logInfo(s"[getCostOfCachedBlock] Executor $executorId $blockId get cost $cost")

    new Cost(blockId, cost.toDouble)
  }
}
