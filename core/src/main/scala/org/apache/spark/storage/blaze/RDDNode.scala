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

import scala.collection.mutable



/**
 * Each RDDNode has an ordered set of jobIds and stageIds that it is referenced.
 *
 * @param rddId
 * @param isShuffledRDD
 * @param callSite
 * @param name
 */
class RDDNode(val rddId: Int,
              val isShuffledRDD: Boolean,
              val callSite: String,
              val name: String) extends Logging {

  private val refJobIds = new mutable.HashSet[Int]()
  private val refStages = new mutable.HashSet[Int]()

  def getReferencedJobs(): mutable.HashSet[Int] = {
    refJobIds
  }

  def getReferencedStages(): mutable.HashSet[Int] = {
    refStages
  }

  def addReferencedJob(jobId: Int): Unit = {
    refJobIds.add(jobId)
  }

  def addReferencedStage(stageId: Int): Unit = {
    refStages.add(stageId)
  }

  override def toString: String = {
    s"RDD $rddId refStages $refStages refJobs $refJobIds"
  }
}
