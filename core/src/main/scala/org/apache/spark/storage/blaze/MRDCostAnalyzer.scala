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

private[spark] class MRDCostAnalyzer(costType: String,
                                     rddJobDag: RDDJobDag,
                                     metricTracker: BlazeMetricTracker)
  extends CostAnalyzer(costType, rddJobDag, metricTracker) with Logging {

  override def getCostOfCachedBlock(executorId: String, blockId: BlockId): Cost = {
    val refDist = rddJobDag.getRefDist(blockId)

    if (rddJobDag.isHidden(blockId)) {
      // deprioritize dead block
      logInfo(s"[BLAZE] [getCostOfCachedBlock] MRD: Executor $executorId $blockId get cost -1.0")
      new Cost(blockId, -1.0)
    } else if (refDist == 0) {
      // block with the smallest refdist = largest cost
      logInfo(s"[BLAZE] [getCostOfCachedBlock] MRD: Executor $executorId $blockId get cost 100")
      new Cost(blockId, 100)
    } else {
      // block with the largest refdist = smallest cost (0 ~ 1)
      logInfo(s"[BLAZE] [getCostOfCachedBlock] MRD: Executor $executorId $blockId get cost" +
        s" ${1/refDist}")
      new Cost(blockId, 1/refDist)
    }
  }
}
