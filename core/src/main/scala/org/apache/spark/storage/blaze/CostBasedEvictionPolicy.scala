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

import scala.collection.mutable.ListBuffer

private[spark] class CostBasedEvictionPolicy(val costAnalyzer: CostAnalyzer)
  extends EvictionPolicy with Logging {

  def getSortedMemBlockCostList(executorId: String): ListBuffer[Cost] = {
    if (costAnalyzer.sortedMemBlockByCompCost.get() == null) {
      // there are no cached blocks in whole cluster now
      return new ListBuffer[Cost]
    }

    // sort by cost, in ascending order
    costAnalyzer.update
    val executorIdToCosts = costAnalyzer.sortedMemBlockByCompCost.get()

    if (executorIdToCosts.contains(executorId)) {
      // find the right (executorId, sorted cost list) and apply func
      executorIdToCosts(executorId)
    } else {
      new ListBuffer[Cost]
    }
  }
}




