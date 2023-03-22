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

private[spark] class SizeCostAnalyzer(costType: String,
                                      rddJobDag: RDDJobDag,
                                      metricTracker: BlazeMetricTracker)
  extends CostAnalyzer(costType, rddJobDag, metricTracker) with Logging {

  override def getCostOfCachedBlock(executorId: String, blockId: BlockId): Cost = {
    // victim = block with smallest cost
    // = block with largest size
    // for blocks with unknown size, we return -1
    val size = metricTracker.memBlockIdToSizeMap.getOrDefault(blockId, -1L)

    new Cost(blockId, 1/size)
  }
}
