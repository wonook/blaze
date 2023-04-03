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
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.storage.blaze.RDDJobDag.buildCachedReverseDependency
import org.apache.spark.storage.{BlockId, RDDBlockId}
import org.json4s.jackson.JsonMethods

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.{Map, mutable}
import scala.io.Source

// Liveness
object Liveness extends Enumeration {
  val Live = Value("Live")
  val Dead = Value("Dead")
}

/**
 * A DAG that contains all the logical dependency information of this application.
 * Cumulatively updated whenever a job is submitted.
 * Created regardless of replacement policies.
 *
 * @param dependency map of [Parent RDD ID: Set of children RDD ID]
 * @param reverseDependency map of [Child : Parent set]
 * @param metricTracker contains all the physical information of each RDD.
 * @param lastProfiledJobId until when profiling has been covered
 */
class RDDJobDag(val dependency: mutable.Map[Int, mutable.Set[Int]],
                val reverseDependency: mutable.Map[Int, mutable.HashSet[Int]],
                val rddIdToRddNodeMap: mutable.Map[Int, RDDNode],
                val jobIdToStageIdsMap: mutable.Map[Int, mutable.HashSet[Int]],
                val metricTracker: BlazeMetricTracker,
                var lastProfiledJobId: Int,
                var lastProfiledStageId: Int,
                val conf: SparkConf) extends Logging {

  var currentJobId = -1
  var pastStages = new mutable.HashSet[Int]()
  var currentStages = new mutable.HashSet[Int]()
  // for filtering out RDDs involved in HDFS read or shuffle
  val stageRDDIds = new mutable.HashSet[Int]()
  val shuffledRDDIds = new mutable.HashSet[Int]()

  // stageId to this stage's cached child:cached ancestors RDD ID map.
  // only contains RDDs marked as cached.
  // cleared at every job submission.
  // used at aging
  val jobWideStageIdToCachedDependencyMap =
  new mutable.HashMap[Int, mutable.Map[Int, mutable.HashSet[Int]]]()
  val jobWideStageIdToCachedReverseDependencyMap =
    new mutable.HashMap[Int, mutable.Map[Int, mutable.HashSet[Int]]]()

  // stageId to this stage's child:parents RDD ID map.
  // cleared at every job submission.
  val jobWideStageIdToStageReverseDependencyMap =
    new mutable.HashMap[(Int, Int), mutable.Map[Int, mutable.HashSet[Int]]]()

  // record application-level cached RDDs
  val userCachedRDDs = new mutable.HashSet[Int]()
  // for reuse-based autocaching: RDDs that are reused, app-wide
  var reusedRDDs = new mutable.HashSet[Int]()
  // for utility-based lazy autocaching: (app-wide)
  // stage ID to (reused RDDs in the stage, reused RDDs with the highest utility)
  val stageIdToReusedRDDsAndHighestUtilRDD =
  new mutable.HashMap[Int, (Set[Int], mutable.Set[Int])]()

  def getRDDNode(rddId: Int): RDDNode = {
    rddIdToRddNodeMap(rddId)
  }

  private def blockIdToRDDId(blockId: BlockId): Int = {
    blockId.asRDDId.get.rddId
  }

  private def getBlockId(rddId: Int, blockId: BlockId): BlockId = {
    val index = blockId.name.split("_")(2).toInt
    RDDBlockId(rddId, index)
  }

  def isCurrentStage(stageId: Int): Boolean = {
    // is one of currently executing stages
    currentStages.foreach(currStageId => {
      if (stageId == currStageId) {
        return true
      }
    })

    false
  }

  def initUtilityBasedLazyAutoCaching(): Unit = {
    val entries = jobWideStageIdToStageReverseDependencyMap.iterator
    while (entries.hasNext) {
      val ((stageId, stageRDDId), stageWideReverseDep) = entries.next()
      // for each stage in this job that this RDD is involved
      // start DFS traversal from the RDD that this stage runs on
      var highestUtilReusedRDDs =
      dfsStageWideReuse(stageRDDId, new mutable.HashSet[Int](), stageId, stageWideReverseDep)

      val reusedRDDsInStage = stageWideReverseDep.keySet.intersect(reusedRDDs).toSet
      highestUtilReusedRDDs = highestUtilReusedRDDs.intersect(reusedRDDsInStage)
      /*
      if (highestUtilReusedRDDs.contains(5)) {
        highestUtilReusedRDDs.remove(5)
        highestUtilReusedRDDs.add(2)
      }

      if (highestUtilReusedRDDs.contains(19)) {
        highestUtilReusedRDDs.remove(19)
        highestUtilReusedRDDs.add(15)
      }

      if (highestUtilReusedRDDs.contains(11)) {
        highestUtilReusedRDDs.remove(11)
        highestUtilReusedRDDs.add(2)
      }

      if (highestUtilReusedRDDs.contains(29)) {
        highestUtilReusedRDDs.remove(29)
        highestUtilReusedRDDs.add(25)
      }
      */

      stageIdToReusedRDDsAndHighestUtilRDD.put(stageId, (reusedRDDsInStage, highestUtilReusedRDDs))

      /*
      logInfo(s"[BLAZE] [initLazyAutoCaching] In stage $stageId: " +
        s"stageWideReverseDep.keySet ${stageWideReverseDep.keySet} " +
        s"reusedRDDs $reusedRDDs " +
        s"reusedRDDsInStage $reusedRDDsInStage " +
        s"highestUtilReusedRDDs $highestUtilReusedRDDs")
      */
      logInfo(s"[BLAZE] [initLazyAutoCaching] stage $stageId " +
        s"highestUtilReusedRDDs $highestUtilReusedRDDs")
    }

  }

  /**
   * Deprecated.
   *
   * @param rddId
   * @param visited
   * @param stageId
   * @param stageWideReverseDep
   * @return
   */
  def dfsStageWideReuse(rddId: Int,
                        visited: mutable.Set[Int],
                        stageId: Int,
                        stageWideReverseDep: mutable.Map[Int, mutable.HashSet[Int]]):
  mutable.Set[Int] = {
    if (visited.contains(rddId)) {
      return visited
    }

    visited.add(rddId)

    if (reusedRDDs.contains(rddId)) {
      logInfo(s"[BLAZE] [dfsForLazyAutoCaching] stage $stageId RDD$rddId is reused: stopping DFS " +
      s"visited before returning $visited")
      return visited
    }

    if (stageWideReverseDep.contains(rddId) && stageWideReverseDep(rddId).nonEmpty) {
      // for each parent in this stage
      for (parentRDDId <- stageWideReverseDep(rddId)) {
        // Proceed to DFS until we add all reused parents
        if (reusedRDDs.contains(parentRDDId)) {
          visited.add(parentRDDId)
          logInfo(s"[BLAZE] [dfsForLazyAutoCaching] " +
            s"stage $stageId RDD$rddId's parent RDD$parentRDDId is reused: stopping DFS")
          // don't return here - we need info of all parents
        } else {
          logInfo(s"[BLAZE] [dfsForLazyAutoCaching] " +
            s"stage $stageId RDD$rddId's parent RDD$parentRDDId is not reused")
          dfsStageWideReuse(parentRDDId, visited, stageId, stageWideReverseDep)
            .foreach(visitedRDDId => visited.add(visitedRDDId))
        }
      }
    }

    visited
  }

  def logCheckHidden(blockId: BlockId,
                     isHidden: Boolean,
                     cachedInMem: mutable.Set[BlockId],
                     cachedInDisk: mutable.Set[BlockId],
                     totalVisited: mutable.Set[BlockId]): Unit = {
    val retBlocksInMem = new StringBuilder
    cachedInMem.map(memBlockId => s"$memBlockId ").foreach(s => retBlocksInMem.append(s))
    val retBlocksInDisk = new StringBuilder
    cachedInDisk.map(diskBlockId => s"$diskBlockId ").foreach(s => retBlocksInDisk.append(s))
    val retBlocksVisited = new StringBuilder
    totalVisited.map(visitedBlockId => s"$visitedBlockId ").foreach(s => retBlocksVisited.append(s))

    logInfo(s"[BLAZE] [CheckHidden] $blockId: isHidden=$isHidden in stage(s) $currentStages " +
    s"cached: ${cachedInMem.size} ${retBlocksInMem.toString()} " +
    s"persisted: ${cachedInDisk.size} ${retBlocksInDisk.toString()} " +
    s"total visited: ${totalVisited.size} ${retBlocksVisited.toString()}")
  }

  /**
   * Dead: blockId is not reachable from the given current blockId
   * Live: blockId is reachable from the given blockId
   * @param blockId
   * @return
   */
  def isHidden(blockId: BlockId): Boolean = {
    val rddId = blockIdToRDDId(blockId)
    val index = blockId.name.split("_")(2).toInt

    val reachable = new mutable.HashSet[BlockId]()
    val cached = new mutable.HashSet[BlockId]()
    val persisted = new mutable.HashSet[BlockId]()

    // child:parents map
    val entries = jobWideStageIdToStageReverseDependencyMap.iterator
    while (entries.hasNext) {
      val ((stageId, stageRDDId), reverseDep) = entries.next()
      // for each current stage in this job that this RDD is included
      if (currentStages.contains(stageId) && reverseDep.contains(rddId)) {
        val stageRDDBlockId = RDDBlockId(stageRDDId, index)
        // start DFS traversal from the stage RDD block
        val (r, c, p) = dfsCheckHidden(stageRDDBlockId, blockId, reverseDep)

        // keep descendants only
        c.remove(blockId)
        p.remove(blockId)

        r.foreach(reachableBlockId => reachable.add(reachableBlockId))
        c.foreach(cachedBlockId => cached.add(cachedBlockId))
        p.foreach(persistedBlockId => persisted.add(persistedBlockId))

        // blockId is reachable (= not hidden) in this stage
        if (reachable.contains(blockId)) {
          logCheckHidden(blockId, false, cached, persisted, reachable)
          return false
        }
      }
    }

    // blockId is hidden in this stage
    if (cached.nonEmpty || persisted.nonEmpty) {
      logCheckHidden(blockId, true, cached, persisted, reachable)
      return true
    }

    // when no current stage(s) involve blockId
    // which has no cached or persisted descendants, we count it as not hidden
    logCheckHidden(blockId, false, cached, persisted, reachable)
    false
  }

  /**
   * Check whether the given block is reachable in the stage
   * by doing DFS on the stage DAG from the stage RDD (leaf of the stage DAG).
   * The search stops when cached or persisted block is reached
   * for each independent upward paths.
   * If blockId is included in the reachable set, it is reachable.
   * Otherwise, it is hidden.
   *
   * @param stageRDDBlockId the leaf block of this stage DAG. starting point of DFS traversal.
   * @param targetBlockId of the block that we check reachability
   * @param reverseDep stage-wide child:parent logical dependency
   * @return reachable, cached, persisted blocks
   */
  def dfsCheckHidden(stageRDDBlockId: BlockId,
                     targetBlockId: BlockId,
                     reverseDep: mutable.Map[Int, mutable.HashSet[Int]]):
  (mutable.Set[BlockId], mutable.Set[BlockId], mutable.Set[BlockId]) = {
    val visited = new mutable.HashSet[BlockId]
    val waitingForVisit = new mutable.ArrayStack[BlockId]

    val reachable = new mutable.HashSet[BlockId]()
    val cached = new mutable.HashSet[BlockId]()
    val persisted = new mutable.HashSet[BlockId]()

    def visit(blockId: BlockId) {
      if (!visited(blockId)) {
        visited += blockId
        reachable += blockId

        if (blockId == targetBlockId) {
          return
        }

        val isInMem = metricTracker.memBlockIdToSizeMap.containsKey(blockId)
        val isInDisk = metricTracker.diskBlockIdToMetadataMap.containsKey(blockId)
        if (isInMem) {
          cached.add(blockId)
          return
        } else if (isInDisk) {
          persisted.add(blockId)
          return
        }

        // this block is not materialized anywhere, and has cached ancestors
        val rddId = blockIdToRDDId(blockId)
        val index = blockId.name.split("_")(2).toInt
        if (reverseDep.contains(rddId) && reverseDep(rddId).nonEmpty) {
          // for each parent in this stage
          for (parentRDDId <- reverseDep(rddId)) {
            // Proceed to DFS until we find cached or persisted ancestor
            val parentBlockId = RDDBlockId(parentRDDId, index)
            val isInMem = metricTracker.memBlockIdToSizeMap.containsKey(parentBlockId)
            val isInDisk = metricTracker.diskBlockIdToMetadataMap.containsKey(parentBlockId)

            if (isInMem) {
              reachable.add(parentBlockId)
              cached.add(parentBlockId)
            } else if (isInDisk) {
              reachable.add(parentBlockId)
              persisted.add(parentBlockId)
            } else {
              waitingForVisit.push(parentBlockId)
            }
          }
        }
      }
    }

    // DFS upward from the leaf of this stage
    waitingForVisit.push(stageRDDBlockId)
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.pop())
    }

    (reachable, cached, persisted)
  }

  /*
  private def dfsStageWideLivenessNaive(blockId: BlockId,
                                              targetBlockId: BlockId,
                                              visited: mutable.Set[BlockId],
                                              stageId: Int,
                                              stageWideDep: mutable.Map[Int, mutable.HashSet[Int]],
                                              inMem: mutable.Set[BlockId],
                                              inDisk: mutable.Set[BlockId]):
  (mutable.Set[BlockId], mutable.Set[BlockId], mutable.Set[BlockId]) = {

    // already visited
    if (visited.contains(blockId)) {
      return (visited, inMem, inDisk)
    }

    visited.add(blockId)

    val isInMem = metricTracker.memBlockIdToSizeMap.containsKey(blockId)
    val isInDisk = metricTracker.diskBlockIdToMetadataMap.containsKey(blockId)

    if (isInMem) {
      logInfo(s"[BLAZE] [dfsGetLiveness] (targetBlockId $targetBlockId) stage $stageId " +
        s"$blockId is materialized in memory: " +
        s"stopping DFS for this path")
      inMem.add(blockId)
      return (visited, inMem, inDisk)
    } else if (isInDisk) {
      logInfo(s"[BLAZE] [dfsGetLiveness] (targetBlockId $targetBlockId) stage $stageId " +
        s"$blockId is materialized in disk: " +
        s"stopping DFS for this path")
      inDisk.add(blockId)
      return (visited, inMem, inDisk)
    }

    // this block is not materialized, and has child blocks
    val rddId = blockIdToRDDId(blockId)
    if (stageWideDep.contains(rddId) && stageWideDep(rddId).nonEmpty) {
      // for each parent in this stage
      for (childRDDId <- stageWideDep(rddId)) {
        // Proceed to DFS until we find materialized parent
        val index = blockId.name.split("_")(2).toInt
        val childBlockId = RDDBlockId(childRDDId, index)
        visited.add(childBlockId)

        val isInMem = metricTracker.memBlockIdToSizeMap.containsKey(childBlockId)
        val isInDisk = metricTracker.diskBlockIdToMetadataMap.containsKey(childBlockId)

        if (isInMem) {
          logInfo(s"[BLAZE] [dfsGetLiveness] (targetBlockId $targetBlockId) stage $stageId " +
            s"$blockId's child $childBlockId is materialized in memory: " +
            s"stopping DFS for this path")
          inMem.add(childBlockId)
        } else if (isInDisk) {
          logInfo(s"[BLAZE] [dfsGetLiveness] (targetBlockId $targetBlockId) stage $stageId " +
            s"$blockId's child $childBlockId is materialized in disk: " +
            s"stopping DFS for this path")
          inDisk.add(childBlockId)
        } else {
          val (v, inM, inD) = dfsStageWideLivenessNaive(childBlockId,
            targetBlockId, stageId, stageWideDep, inMem, inDisk, visited)
          v.foreach(visitedRDDId => visited.add(visitedRDDId))
          inM.foreach(inMemRDDId => inMem.add(inMemRDDId))
          inD.foreach(inDiskRDDId => inDisk.add(inDiskRDDId))
        }
      }
    }

    (visited, inMem, inDisk)
  }
  */

  def updateIntraJobCachedReverseDep(cachedStageDep: mutable.Map[Int, mutable.HashSet[Int]]):
  mutable.Map[Int, mutable.HashSet[Int]] = {
    buildCachedReverseDependency(cachedStageDep)
  }

  def updateIntraJobDependency(stageId: Int,
                               stageRddId: Int,
                               newStageDep: mutable.Map[Int, mutable.HashSet[Int]]): Unit =
    synchronized {
      // Add stageId:reverseStageDep inside this job
      val newStageReverseDep = RDDJobDag.buildReverseDependency(newStageDep)
      jobWideStageIdToStageReverseDependencyMap.put((stageId, stageRddId), newStageReverseDep)
    }

  private val autoCaching = conf.get(BlazeParameters.AUTOCACHING)
  private val lazyAutoCaching = conf.get(BlazeParameters.LAZY_AUTOCACHING)
  private val costFunction = conf.get(BlazeParameters.COST_FUNCTION)
  private val isProfileRun = conf.get(BlazeParameters.IS_PROFILE_RUN)

  /**
   * Called for each stage of a newly submitted job.
   *
   * 1) Add an entry of RDDNode for a newly added RDD
   * 2) Update the dependency and reverse dependency between RDDs
   *
   * Note that for each RDD, both its RDDNode entry and dependency
   * should always be updated in sync.
   *
   * @param jobId of the newly submitted job
   * @param stageId of this job
   * @param newStageDep RDD dependency of the stage with which we're updating RDDJobDag
   */
  def updateAppWideDependency(jobId: Int, stageId: Int, stageRDDId: Int,
                              newStageDep: Map[Int, mutable.Set[Int]]): Unit = synchronized {

    if (jobId > lastProfiledJobId) {
      // for LRC and MRD
      if (jobIdToStageIdsMap.contains(jobId)) {
        val stages = jobIdToStageIdsMap(jobId)
        stages.add(stageId)
        jobIdToStageIdsMap.put(jobId, stages)
      } else {
        val stages = new mutable.HashSet[Int]()
        stages.add(stageId)
        jobIdToStageIdsMap.put(jobId, stages)
      }

      // Update the dependency and RDDNode entries
      newStageDep.foreach(entry => {
        val parentRDDId = entry._1
        val childrenRDDIds = entry._2

        // 1. Update dependency
        // add parent
        if (!dependency.contains(parentRDDId)) {
          dependency(parentRDDId) = new mutable.HashSet[Int]()
        }

        // add children
        childrenRDDIds.foreach(childRDDId => {
          if (!dependency(parentRDDId).contains(childRDDId)) {
            dependency(parentRDDId).add(childRDDId)
          }
        })

        logInfo(s"[BLAZE] [Job-Wide DAG Construction] Updating Dependency:\t" +
          s"RDD$parentRDDId child RDDs ${dependency(parentRDDId)}")


        // 2. Updating RDDNode entries
        // add parent
        if (!rddIdToRddNodeMap.contains(parentRDDId)) {
          rddIdToRddNodeMap.put(parentRDDId,
            new RDDNode(parentRDDId, false, null, null))
        }
        val parentRDDNode = rddIdToRddNodeMap(parentRDDId)
        parentRDDNode.addReferencedJob(jobId)
        parentRDDNode.addReferencedStage(stageId)
        logInfo(s"[BLAZE] [Job-Wide DAG Construction] Updating RDDNode:\t" +
          s"RDD$parentRDDId refJob $jobId refStage $stageId")

        // add children
        childrenRDDIds.foreach(childRDDId => {
          if (!rddIdToRddNodeMap.contains(childRDDId)) {
            rddIdToRddNodeMap.put(childRDDId,
              new RDDNode(childRDDId, false, null, null))
          }

          val childRDDNode = rddIdToRddNodeMap(childRDDId)
          childRDDNode.addReferencedJob(jobId)
          childRDDNode.addReferencedStage(stageId)
          logInfo(s"[BLAZE] [Job-Wide DAG Construction] Updating RDDNode:\t" +
            s"RDD$childRDDId refJob $jobId refStage $stageId")
        })
      })

      if (autoCaching || lazyAutoCaching) {
        // update reused RDDs
        rddIdToRddNodeMap.foreach(entry => {
          val rddId = entry._1
          val rddNode = entry._2
          val refStages = rddNode.getReferencedStages()
          var stagesWithChild = new mutable.HashSet[Int]()
          // that have children in different stages
          if (dependency.contains(rddId)) {
            dependency(rddId).foreach(childRDDId => {
              val childRDDNode = rddIdToRddNodeMap(childRDDId)
              val childRefStages = childRDDNode.getReferencedStages()
              stagesWithChild = stagesWithChild.union(refStages.intersect(childRefStages))
            })
          }

          if (stagesWithChild.size > 1) {
            reusedRDDs.add(rddId)
          }
        })

        reusedRDDs.remove(stageRDDId)

        rddIdToRddNodeMap.foreach(entry => {
          val rddId = entry._1
          if (userCachedRDDs.contains(rddId)) {
            reusedRDDs.add(rddId)
          }
        })

        logInfo(s"[BLAZE] [ReuseBasedAutoCaching] RDDs registered as reused RDD $reusedRDDs")
      }

      // Update reverse dependency
      val newStageReverseDep = RDDJobDag.buildReverseDependency(newStageDep)
      newStageReverseDep.foreach(entry => {
        val childRDDId = entry._1
        val parentRDDIds = entry._2

        if (!reverseDependency.contains(childRDDId)) {
          reverseDependency(childRDDId) = new mutable.HashSet[Int]()
        }

        parentRDDIds.foreach(parentRDDId => {
          if (!reverseDependency(childRDDId).contains(parentRDDId)) {
            reverseDependency(childRDDId).add(parentRDDId)
          }
        })
      })
    }
  }

  override def toString: String = {
    val sb = new StringBuilder()
    sb.append("--------------------------------------------\n")
    for ((key, v) <- dependency) {
      sb.append(s"[$key -> $v]\n")
    }
    sb.append("--------------------------------------------\n")
    sb.toString()
  }

  private def getLiveCachedAncestors(blockId: BlockId): mutable.HashSet[BlockId] = {
    val liveCachedAncestors = new mutable.HashSet[BlockId]()
    val rddId = blockIdToRDDId(blockId)
    val index = blockId.name.split("_")(2).toInt

    // child:parents map
    val entries = jobWideStageIdToCachedReverseDependencyMap.iterator
    while (entries.hasNext) {
      val (stageId, cachedReverseDep) = entries.next()
      // for cached ancestors in current stages
      if (currentStages.contains(stageId) &&
        cachedReverseDep.contains(rddId) && cachedReverseDep(rddId).nonEmpty) {
        val cachedAncestors = cachedReverseDep(rddId)
        cachedAncestors.foreach(ancestorRDDId => {
          val ancestorBlockId = RDDBlockId(ancestorRDDId, index)
          val hidden = isHidden(ancestorBlockId)
          val isInMem = metricTracker.memBlockIdToSizeMap.containsKey(ancestorBlockId)
          // live == not hidden and cached
          if (!hidden && isInMem) {
            liveCachedAncestors.add(ancestorBlockId)
          }
        })
      }
    }

    val sb = new StringBuilder
    liveCachedAncestors.map(ancestorRDDId => s"$ancestorRDDId ").foreach(s => sb.append(s))
    logInfo(s"[BLAZE] [getLiveCachedAncestors] Stage $currentStages " +
      s"$blockId's live cached or persisted ancestors ${sb.toString()}")

    liveCachedAncestors
  }


  private def getCachedAncestorRDDs(blockId: BlockId): mutable.HashSet[Int] = {
    val cachedAncestors = new mutable.HashSet[Int]()
    val rddId = blockIdToRDDId(blockId)

    // child:parents map
    val entries = jobWideStageIdToCachedReverseDependencyMap.iterator
    while (entries.hasNext) {
      val (stageId, cachedReverseDep) = entries.next()
      // for cached ancestors in current stages
      if (currentStages.contains(stageId) &&
        cachedReverseDep.contains(rddId) && cachedReverseDep(rddId).nonEmpty) {
        cachedReverseDep(rddId).foreach(ancestorRDDId => cachedAncestors.add(ancestorRDDId))
      }
    }

    val sb = new StringBuilder
    cachedAncestors.map(ancestorRDDId => s"$ancestorRDDId ").foreach(s => sb.append(s))
    logInfo(s"[BLAZE] [getCachedAncestorRDDs] Stage $currentStages " +
      s"$blockId's marked as cached ancestor RDDs ${sb.toString()}")

    cachedAncestors
  }

  /**
   * Get cached or persisted ancestors which are currently might not be hidden by other blocks,
   * but will be hidden by cache insertion of blockId.
   * @param blockId that hides the ancestors
   * @return
   */
  private def getCachedAncestorsHiddenBy(blockId: BlockId): mutable.HashSet[BlockId] = {
    val hiddenCachedAncestors = new mutable.HashSet[BlockId]()
    val cachedAncestors = getCachedAncestorRDDs(blockId)

    val rddId = blockIdToRDDId(blockId)
    val index = blockId.name.split("_")(2).toInt
    val totalVisited = new mutable.HashSet[BlockId]()

    // child:parents map
    val entries = jobWideStageIdToStageReverseDependencyMap.iterator
    while (entries.hasNext) {
      val ((stageId, stageRDDId), reverseDep) = entries.next()
      // for each current stage in this job that this RDD is included
      if (currentStages.contains(stageId) && reverseDep.contains(rddId)) {
        val stageRDDBlockId = RDDBlockId(stageRDDId, index)
        // start DFS traversal from the stage RDD block
        val (v, _, _) = dfsCheckHidden(stageRDDBlockId, blockId, reverseDep)

        v.foreach(visitedBlockId => totalVisited.add(visitedBlockId))
      }
    }

    // hidden ancestors = those that were not reachable while the target blockId is visited
    cachedAncestors.foreach(ancestorRDDId => {
      val ancestorBlockId = RDDBlockId(ancestorRDDId, index)
      if (!totalVisited.contains(ancestorBlockId)) {
        hiddenCachedAncestors.add(ancestorBlockId)
      }
    })

    val sb = new StringBuilder
    hiddenCachedAncestors.map(ancestorRDDId => s"$ancestorRDDId ").foreach(s => sb.append(s))
    logInfo(s"[BLAZE] [getCachedAncestorsHiddenBy] Stage $currentStages " +
      s"cache insertion of $blockId will hide ${sb.toString()} which are marked as cached")

    hiddenCachedAncestors
  }

  private def stoppingCond(parentBlockId: BlockId, blockId: BlockId): Boolean = {
    val parentRDDId = blockIdToRDDId(parentBlockId)

    if (userCachedRDDs.contains(parentRDDId)) {
      if (costFunction.contains("GD")) {
        /*
        // remove fake priorities
        if (getCachedAncestorsHiddenBy(blockId).contains(parentBlockId)) {
          metricTracker.memBlockIdToCompCostMap.put(parentBlockId, 0)
          return false
        } else {
          return true
        }
        */
        // Direct application of GD that uses snapshot of computation cost
        return (metricTracker.memBlockIdToSizeMap.containsKey(parentBlockId)
        || metricTracker.diskBlockIdToMetadataMap.containsKey(parentBlockId))
      } else {
        // we're calculating snapshot of computation cost,
        // i.e. time to compute a block from its nearest cached ancestors
        return metricTracker.memBlockIdToSizeMap.containsKey(parentBlockId)
      }
    }

    // if not marked as cached, don't stop
    false
  }

  def dfsCalculateCompCost(origBlockId: BlockId):
  (ListBuffer[String], ListBuffer[Long]) = {
    val partialLineage: ListBuffer[String] = mutable.ListBuffer[String]()
    val times: ListBuffer[Long] = mutable.ListBuffer[Long]()

    val visited = new mutable.HashSet[BlockId]()
    val waitingForVisit = new mutable.ArrayStack[BlockId]()

    def visit(blockId: BlockId) {
      if (!visited(blockId)) {
        visited += blockId

        // 1. Unrolling time
        val unrolledBlockId = blockId.name
        val latestUnrollingTime = metricTracker.blockIdToUnrollingTimeMap
          .getOrDefault(unrolledBlockId, 0L)
        partialLineage.append(unrolledBlockId)
        times.append(latestUnrollingTime)

        // for all stages in this job we have
        val rddId = blockIdToRDDId(blockId)
        val entries = jobWideStageIdToStageReverseDependencyMap.iterator
        while (entries.hasNext) {
          val ((stageId, _), stageWideReverseDep) = entries.next()
          if (isCurrentStage(stageId) &&
            stageWideReverseDep.contains(rddId) && stageWideReverseDep(rddId).nonEmpty) {
            // for each parent in this stage
            for (parentRDDId <- stageWideReverseDep(rddId)) {
              // 2. Time to derive this block from its parent block
              val parentBlockId = getBlockId(parentRDDId, blockId)
              val key = s"${parentBlockId.name}-${blockId.name}"
              val time = metricTracker.parentBlockIdToBlockIdCompTimeMap.get(key)
              partialLineage.append(key)
              times.append(time)

              // Proceed to DFS until we find a parent who is materialized and live
              if (stoppingCond(parentBlockId, origBlockId)) {
                logInfo(s"[BLAZE] [dfsCompCost] for $origBlockId: stopping DFS "
                  + s"at $parentBlockId which is cached or persisted")
              } else {
                waitingForVisit.push(parentBlockId)
              }
            }
          }
        }
      }
    }

    waitingForVisit.push(origBlockId)
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.pop())
    }

    (partialLineage, times)
  }

  /**
   * Calculates a current-time snapshot of the computation cost,
   * i.e. the time it takes to compute the block from the nearest cached or persisted ancestors.
   *
   * @param blockId that has just been computed.
   * @return
   */
  def calculateCompCost(blockId: BlockId): Long = {
    val (partialLineage, times) = dfsCalculateCompCost(blockId)

    val computationCost = times.sum
    logInfo(s"[BLAZE] [CompCost] $blockId calculated: " +
      s"lineage ${partialLineage.sortBy(_.split("_")(1).toInt)} time sum $computationCost")

    computationCost
  }

  /**
   * Past number of accesses of each cached block.
   * Used as an eviction metric in LFU.
   *
   * @param blockId
   * @return total number this cached block is accessed
   */
  def getAccessCnt(blockId: BlockId): Int = {
    metricTracker.blockIdToAccessCntMap.get(blockId)
  }

  /**
   * Access recency of each cached block.
   * Used as an eviction metric in LRU.
   *
   * @param blockId
   * @return timestamp when this block is cached
   */
  def getRecency(blockId: BlockId): Long = {
    metricTracker.blockIdToTimeStampMap.get(blockId)
  }

  def isProfiled(blockId: BlockId): Boolean = {
    val rddId = blockId.asRDDId.get.rddId

    rddIdToRddNodeMap.contains(rddId) &&
      dependency.contains(rddId) && dependency(rddId).nonEmpty
  }

  /**
   * number of child blocks - cached child blocks
   * in the future of profiled window
   * @param blockId
   * @return
   */
  def notYetComputedChildBlocks(blockId: BlockId): Int = {
    if (isProfileRun || !isProfiled(blockId)) {
      return -1
    }

    val notYetComputedChildBlocks = new mutable.HashSet[BlockId]()
    val intraJobStages = jobIdToStageIdsMap(currentJobId)
    var lastStageId = lastProfiledStageId
    if (lastProfiledStageId < intraJobStages.max) {
      lastStageId = intraJobStages.max
    }
    val currentStageId = getCurrentStage()

    // get refStages of this block:
    val rddId = blockId.asRDDId.get.rddId
    val rddNode = rddIdToRddNodeMap(rddId)
    val refStages = rddNode.getReferencedStages()

    val childRDDs = dependency(rddId)
    childRDDs.foreach(childRDDId => {
      val index = blockId.name.split("_")(2).toInt
      val childBlockId = RDDBlockId(childRDDId, index)
      val childRDDNode = rddIdToRddNodeMap(childRDDId)
      // get refStages of each child block:
      // 1) Add all distinct child blocks, up to n profiled stages:
      childRDDNode.getReferencedStages().foreach(refStageId => {
        if (refStageId >= currentStageId && refStageId <= lastStageId
          && refStages.contains(refStageId)) {
          /*
          // not marked as cached & in multiple stags -> should be count twice
          if (!userCachedRDDs.contains(childRDDId)
          && notYetComputedChildBlocks.contains(s"$childBlockId")) {
            val str = s"$refStageId:$childBlockId"
            notYetComputedChildBlocks.add(str)
          } else {
            notYetComputedChildBlocks.add(s"$childBlockId")
          }
          */
          notYetComputedChildBlocks.add(childBlockId)
        }
      })

      // 2) If the child block is cached (= computed),
      // exclude it from counting this block's refcnt:
      if (metricTracker.memBlockIdToSizeMap.containsKey(childBlockId)) {
        notYetComputedChildBlocks.remove(childBlockId)
      }
    })

    val refCnt = notYetComputedChildBlocks.size
    logInfo(s"[BLAZE] $blockId refcnt $refCnt " +
      s"not yet computed child blocks " +
      s"from stage $currentStageId until stage $lastStageId: " +
      s"$notYetComputedChildBlocks")

    refCnt
  }

  def getCurrentStage(): Int = {
    var currentStage = -1

    if (currentStages.nonEmpty) {
      currentStage = currentStages.max
    } else {
      if (pastStages.nonEmpty) {
        // there exist future referenced stages,
        // but there are no current stages yet
        currentStage = pastStages.max
      } else {
        // there are no past stages either.
        // which indicates that: we're just starting stage 0
        currentStage = 0
      }
    }

    assert(currentStage >= 0)

    currentStage
  }

  def getRefDist(blockId: BlockId): Double = {
    val rddId = blockIdToRDDId(blockId)
    val rddNode = getRDDNode(rddId)
    val currentStage = getCurrentStage()

    // refdist = current stage ~ nearest referenced stage in the future
    // if there are no future referenced stages, refdist is set to -1 (always lowest cost)
    var refDist = -1.0
    val futureRefStages = rddNode.getReferencedStages().diff(pastStages)
    if (futureRefStages.isEmpty) {
      logInfo(s"[BLAZE] refdist: $blockId currentStage $currentStage nearestRefStage None "
        + s"stageDist -1.0")
      return -1.0
    } else if (futureRefStages.contains(currentStage)) {
      logInfo(s"[BLAZE] refdist: $blockId currentStage $currentStage nearestRefStage $currentStage "
        + s"stageDist 0.0")
      return 0.0
    }

    // there exist future referenced stages
    val nearestRefStage = futureRefStages.min

    // count stages between current stage
    // and the nearest referenced stage
    val stagesLeft = new mutable.HashSet[Int]()
    val entries = jobIdToStageIdsMap.iterator
    while (entries.hasNext) {
      val entry = entries.next()
      val jobId = entry._1
      val stages = entry._2

      // for current and future jobs
      if (jobId >= currentJobId) {
        stages.foreach(stage => {
          // for future stages that are in between current ~ nearest referenced
          if (stage > currentStage && stage <= nearestRefStage) {
            stagesLeft.add(stage)
          }
        })
      }
    }

    logInfo(s"[BLAZE] refdist: $blockId currentStage $currentStage " +
      s"nearestRefStage $nearestRefStage "
      + s"stagesBetween $stagesLeft stageDist ${stagesLeft.size}")

    if (stagesLeft.nonEmpty) {
      refDist = stagesLeft.size
    }

    refDist
  }
}

object RDDJobDag extends Logging {
  // Reverse DAG: Key: Child, Values: Parents
  def buildReverseDependency(dep: Map[Int, mutable.Set[Int]]):
  mutable.Map[Int, mutable.HashSet[Int]] = {
    val reverseDependency = new ConcurrentHashMap[Int, mutable.HashSet[Int]]().asScala
    dep.foreach {
      pair =>
        val parentRDDId = pair._1
        val childRDDIds = pair._2
        childRDDIds.foreach (childRDDId => {
          if (!reverseDependency.contains(childRDDId)) {
            reverseDependency(childRDDId) = new mutable.HashSet[Int]()
          }
          reverseDependency(childRDDId).add(parentRDDId)
        })
    }
    reverseDependency
  }

  // Reverse DAG: Key: Child, Values: Parents
  def buildCachedReverseDependency(dep: Map[Int, mutable.Set[Int]]):
  mutable.Map[Int, mutable.HashSet[Int]] = {
    val cachedReverseDep = new mutable.HashMap[Int, mutable.HashSet[Int]]()
    dep.foreach {
      pair =>
        val parentRDDId = pair._1
        val childRDDIds = pair._2
        childRDDIds.foreach (childRDDId => {
          if (!cachedReverseDep.contains(childRDDId)) {
            cachedReverseDep(childRDDId) = new mutable.HashSet[Int]()
          }
          cachedReverseDep(childRDDId).add(parentRDDId)
        })
    }
    cachedReverseDep
  }


  def apply(dagPath: String,
            sparkConf: SparkConf,
            metricTracker: BlazeMetricTracker): Option[RDDJobDag] = {

    val isProfileRun = sparkConf.get(BlazeParameters.IS_PROFILE_RUN)
    val profileNumJobs = sparkConf.get(BlazeParameters.PROFILE_NUM_JOBS)
    val dependency: mutable.Map[Int, mutable.Set[Int]] =
      new ConcurrentHashMap[Int, mutable.Set[Int]]().asScala
    val rddIdToRddNodeMap = new ConcurrentHashMap[Int, RDDNode]().asScala
    val jobIdToStageIdsMap = new mutable.HashMap[Int, mutable.HashSet[Int]]()

    /**
     * This is either a profiling run,
     * or using job-wide future information only:
     * Information are reset at job boundaries.
     */
    if (isProfileRun || dagPath.equals("None")) {
      Option(new RDDJobDag(dependency, mutable.Map(),
        rddIdToRddNodeMap, mutable.Map(), metricTracker, -1, -1, sparkConf))
    } else {
      /**
       * Using profiled application-wide future information
       */
      var profiledJobIdSoFar = 0
      var profiledJobs = -1
      val profiledStages: ListBuffer[Int] = mutable.ListBuffer[Int]()

      for (line <- Source.fromFile(dagPath).getLines) {
        if (profiledJobs < profileNumJobs) {
          val l = line.stripLineEnd

          // Parse history log file, which is in json format
          implicit val formats = org.json4s.DefaultFormats
          val jsonMap = JsonMethods.parse(l).extract[Map[String, Any]]

          if (jsonMap("Event").equals("SparkListenerJobStart")) {
            profiledJobIdSoFar = jsonMap("Job ID").asInstanceOf[BigInt].toInt
            print(s"Profiled Job ID: $profiledJobIdSoFar")
            profiledJobs += 1
          } else if (jsonMap("Event").equals("SparkListenerStageCompleted")) {
            val stageInfo = jsonMap("Stage Info")
              .asInstanceOf[scala.collection.immutable.HashMap[Any, Any]]
            val rdds = stageInfo("RDD Info").asInstanceOf[List[Any]].toIterator
            val stageId = stageInfo("Stage ID").asInstanceOf[BigInt].toInt
            profiledStages += stageId

            // for LRC and MRD
            if (jobIdToStageIdsMap.contains(profiledJobIdSoFar)) {
              val stages = jobIdToStageIdsMap(profiledJobIdSoFar)
              stages.add(stageId)
              jobIdToStageIdsMap.put(profiledJobIdSoFar, stages)
            } else {
              val stages = new mutable.HashSet[Int]()
              stages.add(stageId)
              jobIdToStageIdsMap.put(profiledJobIdSoFar, stages)
            }

            // add vertices
            for (r <- rdds) {
              val rdd = r.asInstanceOf[scala.collection.immutable.HashMap[Any, Any]]

              val rddId = rdd("RDD ID").asInstanceOf[BigInt].toInt
              val name = rdd("Name").asInstanceOf[String]
              val callSite = rdd("Callsite").asInstanceOf[String]
              val parentIds = rdd("Parent IDs").asInstanceOf[List[Any]].toIterator
              var rddNode = new RDDNode(rddId, name.equals("ShuffledRDD"), callSite, name)

              if (rddIdToRddNodeMap.contains(rddId)) {
                rddNode = rddIdToRddNodeMap(rddId)
              }
              rddNode.addReferencedJob(profiledJobIdSoFar)
              rddNode.addReferencedStage(stageId)
              rddIdToRddNodeMap.put(rddId, rddNode)

              logInfo(s"[BLAZE] [App-Wide DAG Construction] Updating RDDNode:\t" +
                s"RDD${rddNode.rddId} ${rddNode.toString}")

              for (parentId <- parentIds) {
                // Update RDDJobDag dependency
                val parentRDDId = parentId.asInstanceOf[BigInt].toInt
                if (!dependency.contains(parentRDDId)) {
                  dependency(parentRDDId) = new mutable.HashSet()
                }
                dependency(parentRDDId).add(rddId)
                logInfo(s"[BLAZE] [App-Wide DAG Construction] Updating " +
                  s"dependency(parent:child):\tRDD$parentRDDId:RDD$rddId")
              }
            }
          }
        }
      }

      // cycle detection
      dependency.keys.foreach {
        rddId =>
          if (dependency(rddId).contains(rddId)) {
            throw new RuntimeException(
              s"dependency has cycle: RDD$rddId: ${dependency(rddId)}")
          }
      }

      profiledJobIdSoFar -= 1
      logInfo(s"[BLAZE] [App-Wide DAG Construction] " +
        s"lastProfiledJobId $profiledJobIdSoFar lastProfiledStageId ${profiledStages.max} " +
        s"profiledStages ${profiledStages.sorted}")
      val rddJobDag = new RDDJobDag(dependency, buildReverseDependency(dependency),
        rddIdToRddNodeMap, jobIdToStageIdsMap,
        metricTracker, profiledJobIdSoFar, profiledStages.max, sparkConf)

      rddJobDag.reverseDependency.keys.foreach {
        rddId =>
          if (rddJobDag.reverseDependency(rddId).contains(rddId)) {
            throw new RuntimeException(
              s"reverseDependency has cycle: RDD$rddId: ${rddJobDag.reverseDependency(rddId)}")
          }
      }

      Option(rddJobDag)
    }
  }
}
