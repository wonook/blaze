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

import org.apache.spark.rpc.{RpcCallContext, RpcEnv}
import org.apache.spark.storage.blaze.BlazeBlockManagerMessages._
import org.apache.spark.storage.{BlockId, BlockManagerMasterEndpoint, RDDBlockId}
import org.apache.spark.{Partition, SparkConf}

import java.util.concurrent.locks.{ReadWriteLock, StampedLock}
import java.util.concurrent.{ConcurrentHashMap, Executors, TimeUnit}
import scala.collection.convert.decorateAsScala._
import scala.collection.mutable.ListBuffer
import scala.collection.{mutable, _}

private[spark] class BlazeRpcEndpoint(override val rpcEnv: RpcEnv,
                                      val isLocal: Boolean,
                                      conf: SparkConf,
                                      blockManagerMasterEndpoint: BlockManagerMasterEndpoint,
                                      val costAnalyzer: CostAnalyzer,
                                      val metricTracker: BlazeMetricTracker,
                                      val cachingPolicy: CachingPolicy,
                                      val evictionPolicy: EvictionPolicy,
                                      val rddJobDag: Option[RDDJobDag])
  extends AbstractBlazeRpcEndpoint {

  logInfo(s"[BLAZE] BlazeRpcEndpoint created")

  blockManagerMasterEndpoint.setBlazeRpcEndpoint(this)

  val autoUnpersist = conf.get(BlazeParameters.AUTOUNPERSIST)
  val reuseBasedAutoCaching = conf.get(BlazeParameters.AUTOCACHING)
  val lazyAutoCaching = conf.get(BlazeParameters.LAZY_AUTOCACHING)
  private val costFun = conf.get(BlazeParameters.COST_FUNCTION)

  val localExecutorLockMap = new ConcurrentHashMap[String, Object]().asScala
  private val blockIdToLockMap = new ConcurrentHashMap[BlockId, ReadWriteLock].asScala

  val scheduler = Executors.newSingleThreadScheduledExecutor()
  val task = new Runnable {
    def run(): Unit = {
      try {
        // Periodically update cost analyzer
        costAnalyzer.update
      } catch {
        case e: Exception =>
          e.printStackTrace()
          throw new RuntimeException(s"Exception while updating cost analyzer: " + e.getMessage)
      }
    }
  }
  scheduler.scheduleAtFixedRate(task, 1, 5, TimeUnit.SECONDS)

  override def onStop(): Unit = {
    scheduler.shutdownNow()
  }

  // Public methods
  def removeExecutor(executorId: String): Unit = {
    logInfo(s"[BLAZE] Remove executor $executorId: " +
      s"Currently does nothing - implement locks for FT")

    /*
    localExecutorLockMap(executorId).synchronized {
      if (executorWriteLockCount.contains(executorId)) {
        val set = executorWriteLockCount.remove(executorId).get
        set.foreach {
          bid => blockIdToLockMap(bid).writeLock().unlock()
        }
      }

      if (executorReadLockCount.contains(executorId)) {
        val blockCounterMap = executorReadLockCount.remove(executorId).get.asScala
        blockCounterMap.foreach {
          entry => val bid = entry._1
            val cnt = entry._2
            for (i <- 1 to cnt.get()) {
              blockIdToLockMap(bid).readLock().unlock()
            }
        }
      }

      // Remove local info
      metricTracker.clearAllMemBlocksFromExecutor(executorId)
    }
     */
  }

  /**
   * For reuse-based autocaching, return true if it is marked as so.
   * For lazy autocaching, return true only when it will not be hidden
   * by later RDDs in the same task.
   * @param rddId
   * @return whether to regard this as a cached rdd
   */
  def isCachedRDD(rddId: Int): Boolean = {
    if (reuseBasedAutoCaching) {
      return reuseBasedAutoCaching(rddId)
    } else if (lazyAutoCaching) {
      return utilityBasedLazyAutoCaching(rddId)
    }

    // should not reach here
    false
  }

  def isUserCachedRDD(rddId: Int): Boolean = {
    rddJobDag.get.userCachedRDDs.contains(rddId)
  }

  def addUserCachedRDD(rddId: Int): Unit = {
    rddJobDag.get.userCachedRDDs.add(rddId)
  }

  def addShuffledRDD(rddId: Int): Unit = {
    rddJobDag.get.shuffledRDDIds.add(rddId)
  }

  def taskFinished(taskId: String): Unit = {
    logInfo(s"[BLAZE] TaskFinished $taskId")
    metricTracker.taskFinished(taskId)
  }

  def taskStarted(taskId: String): Unit = {
    logInfo(s"[BLAZE] TaskStarted $taskId")
    metricTracker.taskStarted(taskId)
  }

  def reuseBasedAutoCaching(rddId: Int): Boolean = {
    if (rddJobDag.get.reusedRDDs.contains(rddId)) {
      logInfo(s"[BLAZE] [ReuseBasedAutoCaching] WILL cache RDD$rddId which is reused")
      return true
    }

    false
  }

  /**
   * Used for utility-based lazy autocaching.
   * @param rddId
   * @return true if re-cache, false otherwise
   */
  def utilityBasedLazyAutoCaching(rddId: Int): Boolean = {
    rddJobDag.get.stageIdToReusedRDDsAndHighestUtilRDD.foreach(entry => {
      val stageId = entry._1
      val (reusedRDDsInStage, highestUtilReusedRDDs) = entry._2
      if (rddJobDag.get.currentStages.contains(stageId)) {
        if (highestUtilReusedRDDs.contains(rddId)) {
          logInfo(s"[BLAZE] [LazyAutoCaching] WILL cache RDD$rddId in stage $stageId " +
            s"dep $reusedRDDsInStage highest util RDDs $highestUtilReusedRDDs")
          return true
        } else if (reusedRDDsInStage.contains(rddId)) {
          // logInfo(s"[BLAZE] [LazyAutoCaching] WON'T cache RDD$rddId. dep $reusedRDDsInStage " +
          //  s"highest util RDDs $highestUtilReusedRDDs")
          return false
        }
      }
    })

    if (rddJobDag.get.reusedRDDs.contains(rddId)) {
      logInfo(s"[BLAZE] [LazyAutoCaching] RDD$rddId is reused " +
        s"but not included in current stage(s)")
    }

    false
  }

  /**
   * Unpersist blocks with zero utility right after stage completion.
   *
   * @param stageId of the completed stage
   */
  def stageCompleted(stageId: Int): Unit = {
    rddJobDag.get.currentStages.remove(stageId)
    rddJobDag.get.pastStages.add(stageId)
  }

  /**
   * By updating jobIdToStageIdsMap at the point of stage submission,
   * it always contains up-to-date stage info, including currently executing stageId(s).
   * Policies that use stage-related metrics as cost, such as MRD,
   * can safely use jobIdToStageIdsMap to calculated cost of cached blocks.
   *
   * @param stageId of the submitted stage
   * @param jobId current job id
   * @param rdd the last RDD of this stage
   * @param partitions of this RDD
   * @param numPartitions of this RDD
   */
  def stageSubmitted(stageId: Int, jobId: Int,
                     rdd: String, partitions: Array[Partition], numPartitions: Int): Unit =
    synchronized {
      logInfo(s"[BLAZE] Stage submitted: $stageId, jobId: $jobId " +
        s"stageId:jobId map so far: ${metricTracker.jobIdToStageIdsMap}")

      metricTracker.stageIdToSubmissionTimeMap.put(stageId, System.currentTimeMillis())

      rddJobDag.get.currentStages.add(stageId)
      if (metricTracker.jobIdToStageIdsMap.contains(jobId)) {
        metricTracker.jobIdToStageIdsMap(jobId).add(stageId)
      } else {
        metricTracker.jobIdToStageIdsMap(jobId) = new mutable.HashSet[Int]()
        metricTracker.jobIdToStageIdsMap(jobId).add(stageId)
      }
    }

  def removeBlockFromExecutor(blockId: BlockId, executorId: String, onDisk: Boolean): Unit = {
    metricTracker.removeBlockFromExecutor(blockId, executorId, onDisk)
  }

  // Private methods

  private def registerLocationAndSizeOfMemBlock(blockId: BlockId, executorId: String,
                                                deserializedSize: Long): Unit = {
    metricTracker.registerLocationAndSizeOfMemBlock(blockId, executorId, deserializedSize)
  }

  private def registerLocationAndMetadataOfDiskBlock(blockId: BlockId, executorId: String,
                                                     serializedSize: Long,
                                                     spillTime: Long): Unit = {
    metricTracker.registerLocationAndMetadataOfDiskBlock(blockId, executorId,
      serializedSize, spillTime)
  }

  private def getParentBlockIds(blockId: BlockId): mutable.HashSet[BlockId] = {
    val parentBlockIds = new mutable.HashSet[BlockId]()
    val rddId = blockId.asRDDId.get.rddId

    if (rddJobDag.get.reverseDependency.contains(rddId)) {
      val partitionId = blockId.name.split("_")(2).toInt

      val parentRDDIds = rddJobDag.get.reverseDependency(rddId)
      parentRDDIds.foreach(parentRDDId => {
        parentBlockIds.add(RDDBlockId(parentRDDId, partitionId))
      })
    }

    val parentBlocks = new StringBuilder
    parentBlockIds.map(blockId => s"$blockId ").foreach(s => parentBlocks.append(s))

    parentBlockIds
  }

  private def handleCaching(blockId: BlockId, executorId: String,
                            onDisk: Boolean, size: Long, time: Long): Unit = {

    assert(blockId.isRDD)

    if (onDisk) {
      registerLocationAndMetadataOfDiskBlock(blockId, executorId, size, time)
    } else {
      registerLocationAndSizeOfMemBlock(blockId, executorId, size)
    }
  }

  private def handleCachingFailure(blockId: BlockId, deserializedSizeSoFar: Long,
                                   executorId: String): Unit = {
    BlazeLogger.cachingFailure(blockId, executorId, deserializedSizeSoFar)
    removeBlockFromExecutor(blockId, executorId, false)
  }

  /**
   * Invariant:
   * For the case of caching in MemoryStore,
   * this function is called only when unrolling the block is successfully finished,
   * thus *accurate* deserialized size of the block is available, for cost calculation.
   * TODO: add size estimation by building assumptions in the profiling phase
   *
   * @param blockId to cache
   * @param deserializedSize of the block
   * @param executorId where this block will be stored
   * @return
   */
  private def handleCachingDecision(blockId: BlockId, deserializedSize: Long,
                                    executorId: String): Boolean = {
    blockIdToLockMap.putIfAbsent(blockId, new StampedLock().asReadWriteLock())

    // Init recency for blocks that are created for the first time in this job
    val latestAccessTime = System.currentTimeMillis()
    if (!metricTracker.blockIdToTimeStampMap.containsKey(blockId)) {
      metricTracker.blockIdToTimeStampMap.put(blockId, latestAccessTime)
      logInfo(s"[BLAZE] init recency for $blockId: $latestAccessTime upon caching")
    }

    val currentJobId = rddJobDag.get.currentJobId

    // Init past frequency blocks that are created for the first time in this job
    if (!metricTracker.blockIdToAccessCntMap.containsKey(blockId)) {
      logInfo(s"[BLAZE] init past frequency for $blockId: 0 upon caching. " +
        s"first created in job $currentJobId")
      metricTracker.blockIdToAccessCntMap.put(blockId, 0)
    }

    // miss penalty, i.e. time to compute this block as it is not present
    val compCost = rddJobDag.get.calculateCompCost(blockId)

    if (costFun.contains("GD")) {
      // For GreedyDual, we don't fetch previously recorded cost which has been aged.
      // we re-assign it at this point of cache insertion,
      // i.e. promotion at cache insertion in GreedDual.
      // Note that there is no consistency problem when other tasks perform victim selection,
      // as the cache insertion of this block is not finished yet.
      val newCost = compCost

      if (newCost <= 0) {
        // if the incoming block has zero cost
        // just discard (= decide not to cache) this
        BlazeLogger.discardDecision(blockId, executorId, newCost, deserializedSize,
          s"$deserializedSize, decided not to cache (cost = 0)")
        return false
      } else {
        // always cache if there is enough space in MemoryStore.
        val oldCost = metricTracker.memBlockIdToCompCostMap.get(blockId)
        metricTracker.memBlockIdToCompCostMap.put(blockId, newCost)
        BlazeLogger.cachingDecision(costFunction,
          blockId, executorId, deserializedSize, oldCost, newCost)
      }
    } else {
      val cost = costAnalyzer.getCostOfCachedBlock(executorId, blockId)

      // always cache if there is enough space in MemoryStore
      BlazeLogger.cachingDecision(costFunction,
        blockId, executorId, deserializedSize, cost.cost, compCost)
    }

    true
  }

  private val costFunction = conf.get(BlazeParameters.COST_FUNCTION)

  /**
   * Decides whether to pick the existing one as a victim.
   *
   * Note that as demand-based policies (LRU and LFU) don't have any insertion policy,
   * incoming block isn't included as a victim candidate.
   * Other policies (LRC, MRD, GD, Blaze) assign replacement states upon insertion,
   * thus incoming block is involved in the victim selection process.
   *
   * @param existingCost
   * @param incomingCost
   * @return true if the existing one is picked as a victim.
   */
  private def pickCachedOneAsVictim(existingCost: Cost, incomingCost: Cost): Boolean = {
    if (costFunction.contains("GD") ||
      costFunction.contains("LRC") || costFunction.contains("MRD")) {
      if (incomingCost.cost >= 0.0) {
        if (existingCost.cost <= incomingCost.cost) {
          logInfo(s"[BLAZE] [VictimSelection] $costFunction: " +
            s"existing ${existingCost.blockId} ${existingCost.cost} < " +
            s"incoming ${incomingCost.blockId} ${incomingCost.cost}")
          return true
        } else {
          return false
        }
      } else {
        // if there is no incoming block, i.e. we're evicting because of the EM memory pressure,
        // just evict lowest cost one
        return true
      }
    } else {
      // LRU, LFU
      return true
    }

    false
  }

  private def aging(costOfCachedBlock: Cost): Unit = {
    if (costOfCachedBlock.cost > 0.0) {
      val rddId = costOfCachedBlock.blockId.asRDDId.get.rddId
      val index = costOfCachedBlock.blockId.name.split("_")(2).toInt
      var entries = rddJobDag.get.jobWideStageIdToCachedReverseDependencyMap.iterator
      val agedAncestors = new mutable.HashSet[BlockId]()

      while (entries.hasNext) {
        val (_, cachedReverseDep) = entries.next()
        if (cachedReverseDep.contains(rddId) && cachedReverseDep(rddId).nonEmpty) {
          // for each cached ancestor in this stage
          for (cachedAncestor <- cachedReverseDep(rddId)) {
            val cachedAncestorBlock = RDDBlockId(cachedAncestor, index)
            // if there are multiple stages in this job, that contain this ancestor,
            // avoid duplicate aging
            if (metricTracker.memBlockIdToCompCostMap.containsKey(cachedAncestorBlock)
              && !agedAncestors.contains(cachedAncestorBlock)) {
              val origCost = metricTracker.memBlockIdToCompCostMap
                .get(cachedAncestorBlock)
              val agedCost = origCost - costOfCachedBlock.cost.toLong
              metricTracker.memBlockIdToCompCostMap.put(cachedAncestorBlock, agedCost)
              agedAncestors.add(cachedAncestorBlock)
              logInfo(s"[BLAZE] [Aging] victim ${costOfCachedBlock.blockId} ${costOfCachedBlock.cost}" +
                s"ancestor $cachedAncestorBlock $origCost aged to $agedCost")
            }
          }
        }
      }

      // age dependent descendant cached blocks
      entries = rddJobDag.get.jobWideStageIdToCachedDependencyMap.iterator
      val agedDescendants = new mutable.HashSet[BlockId]()
      while (entries.hasNext) {
        val (_, cachedDep) = entries.next()
        if (cachedDep.contains(rddId) && cachedDep(rddId).nonEmpty) {
          // for each cached descendant in this stage
          for (cachedDescendant <- cachedDep(rddId)) {
            val cachedDescendantBlock = RDDBlockId(cachedDescendant, index)
            // avoid duplicate aging
            if (metricTracker.memBlockIdToCompCostMap.containsKey(cachedDescendant)
              && !agedDescendants.contains(cachedDescendantBlock)) {
              val origCost = metricTracker.memBlockIdToCompCostMap
                .get(cachedDescendantBlock)
              val agedCost = origCost - costOfCachedBlock.cost.toLong
              metricTracker.memBlockIdToCompCostMap.put(cachedDescendantBlock, agedCost)
              agedDescendants.add(cachedDescendantBlock)
              logInfo(s"[BLAZE] [Aging] victim ${costOfCachedBlock.blockId} ${costOfCachedBlock.cost}" +
                s"descendant $cachedDescendant $origCost aged to $agedCost")
            }
          }
        }
      }
    }
  }

  /**
   * Select victim blocks s.t. their aggregate size is enough to store the incoming block.
   *
   * @param incomingBlockId the block we are freeing space for (*not* the victim block!)
   * @param executorId where this selection is happening
   * @param numBytesToFree target aggregate size we need to free
   * @return list of (victim BlockId, spillToDisk) tuple
   */
  private def handleVictimSelection(incomingBlockId: Option[BlockId],
                                    executorId: String, numBytesToFree: Long, stageId: Int):
  List[(BlockId, Boolean)] = {
    val selectedVictims: mutable.ListBuffer[(BlockId, Boolean)]
    = new ListBuffer[(BlockId, Boolean)]
    var sizeSum = 0L
    var costSum = 0.0
    var spillToDisk = false

    val costList = evictionPolicy.getSortedMemBlockCostList(executorId)
    // incoming block is always a newly created one
    var costOfIncomingBlock = new Cost(RDDBlockId(-1, -1), -1)
    if (incomingBlockId.isDefined && incomingBlockId.get.isRDD) {
      if (costFun.contains("GD")) {
        // we don't have record
        val compCost = rddJobDag.get.calculateCompCost(incomingBlockId.get)
        costOfIncomingBlock = new Cost(incomingBlockId.get, compCost)
      } else {
        costOfIncomingBlock = costAnalyzer.getCostOfCachedBlock(executorId, incomingBlockId.get)
      }
    }

    // if there are no already cached blocks - there's no victims.
    if (!costList.isEmpty) {
      costList.synchronized {
        // for each already cached block
        for (i <- 0 until costList.length) {
          val costOfCachedBlock = costList(i)
          val sizeOfCachedBlock = {
            metricTracker.memBlockIdToSizeMap.getOrDefault(costOfCachedBlock.blockId, 0)
          }

          // for blocks stored in this executor (which asked for eviction)
          if (metricTracker.executorIdToMemBlockIdsMap.get(executorId)
            .contains(costOfCachedBlock.blockId) &&
            sizeOfCachedBlock > 0) {

            if (pickCachedOneAsVictim(costOfCachedBlock, costOfIncomingBlock)) {
              // add another existing one as a victim
              costSum += costOfCachedBlock.cost
              sizeSum += sizeOfCachedBlock
              selectedVictims.append((costOfCachedBlock.blockId, spillToDisk))

              if (costFun.contains("GD")) {
                // log eviction here
                // snapshot of computation cost
                val compCost = rddJobDag.get.calculateCompCost(costOfCachedBlock.blockId)
                BlazeLogger.eviction(costOfCachedBlock.blockId, costFunction, executorId, stageId,
                  costOfCachedBlock.cost, compCost, -1, sizeOfCachedBlock, sizeOfCachedBlock)

                // age dependent ancestor cached blocks
                aging(costOfCachedBlock)
              }
            }

            if (sizeSum >= numBytesToFree) {
              logInfo(s"[BLAZE] [VictimSelection] $costFunction: " +
                s"SizeSum: $sizeSum numBytesToFree: $numBytesToFree " +
                s"incomingBlock: $incomingBlockId selectedVictims $selectedVictims")
              return selectedVictims.toList
            }
          }
        }
      }
    }

    logWarning(s"[BLAZE] [VictimSelection] $costFunction: " +
      s"costList is empty: nothing to evict for incomingBlock $incomingBlockId")
    List.empty
  }

  private def handleEvictionFailure(blockId: BlockId, executorId: String): Unit = {
    BlazeLogger.evictionFailure(blockId, executorId)
  }

  /**
   * The victim block is successfully dropped from MemoryStore.
   * @param blockId of the victim block that is dropped
   * @param executorId where this block is stored
   */
  private def handleEviction(blockId: BlockId, executorId: String, stageId: Int): Unit = {
    if (!costFun.contains("GD")) {
      // actual time spent to spill this block, if spilled
      val spillTime = if (metricTracker.diskBlockIdToMetadataMap.containsKey(blockId)) {
        val metadata = metricTracker.diskBlockIdToMetadataMap.get(blockId)
        metadata.spillTime
      } else -1L

      val serializedSize = if (metricTracker.diskBlockIdToMetadataMap.containsKey(blockId)) {
        val metadata = metricTracker.diskBlockIdToMetadataMap.get(blockId)
        metadata.serializedSize
      } else 0L

      val deserializedSize = if (metricTracker.memBlockIdToSizeMap.containsKey(blockId)) {
        metricTracker.memBlockIdToSizeMap.get(blockId)
      } else 0L

      val cost = costAnalyzer.getCostOfCachedBlock(executorId, blockId)

      // snapshot of computation cost is measured for polices other than Blaze
      val compCost = rddJobDag.get.calculateCompCost(blockId).toDouble

      BlazeLogger.eviction(blockId, costFunction, executorId, stageId,
        cost.cost, compCost, spillTime,
        deserializedSize, serializedSize)
    }

    removeBlockFromExecutor(blockId, executorId, false)
  }

  def handlePromotionDecision(blockId: BlockId, executorId: String): Boolean = {
    false
  }

  override def removeMetadataOfBlocks(stageId: Int, removedBlocks: mutable.Set[BlockId]): Unit = {
    metricTracker.getExecutorIdToMemBlockIdsMap.synchronized {
      metricTracker.getExecutorIdToMemBlockIdsMap.foreach {
        entrySet =>
          val executorId = entrySet._1
          val blockSet = entrySet._2
          val removedMemBlockSet = new mutable.HashSet[(BlockId, String)]()
          localExecutorLockMap(executorId).synchronized {
            blockSet.foreach {
              bid =>
                if (removedBlocks.contains(bid)) {
                  removedMemBlockSet.add((bid, executorId))
                }
            }
            removedMemBlockSet.foreach(pair => {
              BlazeLogger.removeMemBlock(stageId, pair._1, pair._2)
              removeBlockFromExecutor(pair._1, pair._2, false)
            })
          }
      }
    }

    metricTracker.getExecutorIdToDiskBlockIdsMap.synchronized {
      metricTracker.getExecutorIdToDiskBlockIdsMap.foreach {
        entrySet =>
          val executorId = entrySet._1
          val blockSet = entrySet._2
          val removedDiskBlockSet = new mutable.HashSet[(BlockId, String)]()
          localExecutorLockMap(executorId).synchronized {
            blockSet.foreach {
              bid =>
                if (removedBlocks.contains(bid)) {
                  removedDiskBlockSet.add((bid, executorId))
                }
                removedDiskBlockSet.foreach(pair => {
                  BlazeLogger.removeDiskBlock(stageId, pair._1, pair._2)
                  removeBlockFromExecutor(pair._1, pair._2, true)
                })
            }
          }
      }
    }
  }


  override def removeMetadata(removedRdds: Set[Int]): Unit = {
    metricTracker.getExecutorIdToMemBlockIdsMap.synchronized {
      metricTracker.getExecutorIdToMemBlockIdsMap.foreach {
        entrySet =>
          val executorId = entrySet._1
          val blockSet = entrySet._2
          val removeSet = new mutable.HashSet[(BlockId, String)]()
          localExecutorLockMap(executorId).synchronized {
            blockSet.foreach {
              bid =>
                if (removedRdds.contains(bid.asRDDId.get.rddId)) {
                  removeSet.add((bid, executorId))
                }
            }
            removeSet.foreach(pair => {
              BlazeLogger.removeMemBlock(-1, pair._1, pair._2)
              removeBlockFromExecutor(pair._1, pair._2, false)
            })
          }
      }
    }

    metricTracker.getExecutorIdToDiskBlockIdsMap.synchronized {
      metricTracker.getExecutorIdToDiskBlockIdsMap.foreach {
        entrySet =>
          val executorId = entrySet._1
          val blockSet = entrySet._2
          val removeSet = new mutable.HashSet[(BlockId, String)]()
          localExecutorLockMap(executorId).synchronized {
            blockSet.foreach {
              bid =>
                if (removedRdds.contains(bid.asRDDId.get.rddId)) {
                  removeSet.add((bid, executorId))
                }
                removeSet.foreach(pair => {
                  BlazeLogger.removeDiskBlock(-1, pair._1, pair._2)
                  removeBlockFromExecutor(pair._1, pair._2, true)
                })
            }
          }
      }
    }
  }

  // For failure handling - not quite needed for now
  // private val executorReadLockCount =
  //   new ConcurrentHashMap[String, ConcurrentHashMap[BlockId, AtomicInteger]].asScala
  // private val executorWriteLockCount =
  //   new ConcurrentHashMap[String, mutable.Set[BlockId]].asScala

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {

    case CachingDecision(blockId, deserSize, executorId) =>
      localExecutorLockMap.putIfAbsent(executorId, new Object)
      context.reply(handleCachingDecision(blockId, deserSize, executorId))

    case Materialized(blockId, stageId, executorId, time, size, onDisk) =>
      BlazeLogger.materialized(blockId, stageId, executorId, size, onDisk)
      localExecutorLockMap.putIfAbsent(executorId, new Object)
      val lock = localExecutorLockMap(executorId)
      lock.synchronized {
        handleCaching(blockId, executorId, onDisk, size, time)
      }

    case CachingFailure(blockId, deserializedSizeSoFar, executorId) =>
      localExecutorLockMap.putIfAbsent(executorId, new Object)
      val lock = localExecutorLockMap(executorId)
      lock.synchronized {
        handleCachingFailure(blockId, deserializedSizeSoFar, executorId)
      }

    case CacheHit(blockId, executorId, fromRemote, onDisk, rtime) =>
      if (!fromRemote && !onDisk) {
        // promotion at cache hit
        if (costFun.contains("GD")) {
          val newCost = rddJobDag.get.calculateCompCost(blockId)
          metricTracker.memBlockIdToCompCostMap.put(blockId, newCost)
        }

        val currentJobId = rddJobDag.get.currentJobId

        // update past accesses
        assert(metricTracker.blockIdToAccessCntMap.containsKey(blockId))
        val newAccessCnt = metricTracker.blockIdToAccessCntMap.get(blockId) + 1
        metricTracker.blockIdToAccessCntMap.put(blockId, newAccessCnt)
        logInfo(s"[BLAZE] Hit: $blockId past frequency $newAccessCnt " +
          s"(job $currentJobId stage ${rddJobDag.get.currentStages})")

        // update recency
        assert(metricTracker.blockIdToTimeStampMap.containsKey(blockId))
        val latestAccessTime = System.currentTimeMillis()
        metricTracker.blockIdToTimeStampMap.put(blockId, latestAccessTime)
        logInfo(s"[BLAZE] Hit: $blockId recency $latestAccessTime " +
          s"(job $currentJobId stage ${rddJobDag.get.currentStages})")
      }
      BlazeLogger.cacheHit(blockId, executorId, fromRemote, onDisk, rtime)

    case CacheMiss(blockId, executorId) =>
      BlazeLogger.cacheMiss(blockId, executorId)
      // Note that LRU, LFU and LRC only update recency/frequency upon hit,
      // i.e. upon promotion

    case IsCachedRDD(rddId) =>
      context.reply(isCachedRDD(rddId))

    case GetDeserializedSize(blockId, executorId) =>
      localExecutorLockMap.putIfAbsent(executorId, new Object)
      if (metricTracker.memBlockIdToSizeMap.containsKey(blockId)) {
        context.reply(metricTracker.memBlockIdToSizeMap.get(blockId))
      } else {
        context.reply(-1L)
      }

    case GetSerializedSize(blockId, executorId) =>
      localExecutorLockMap.putIfAbsent(executorId, new Object)
      if (metricTracker.diskBlockIdToMetadataMap.containsKey(blockId)) {
        val metadata = metricTracker.diskBlockIdToMetadataMap.get(blockId)
        context.reply(metadata.serializedSize)
      } else {
        throw new RuntimeException("Requested serialized size of a non-existent block")
      }

    case VictimSelection(incomingBlockId, executorId, numBytesToFree, stageId) =>
      localExecutorLockMap.putIfAbsent(executorId, new Object)
      val lock = localExecutorLockMap(executorId)
      lock.synchronized {
        context.reply(handleVictimSelection(incomingBlockId, executorId, numBytesToFree, stageId))
      }

    case EvictionFailure(blockId, executorId) =>
      localExecutorLockMap.putIfAbsent(executorId, new Object)
      val lock = localExecutorLockMap(executorId)
      lock.synchronized {
        handleEvictionFailure(blockId, executorId)
      }

    case EvictionDone(blockId, executorId, stageId) =>
      localExecutorLockMap.putIfAbsent(executorId, new Object)
      val lock = localExecutorLockMap(executorId)
      lock.synchronized {
        handleEviction(blockId, executorId, stageId)
      }

    case PromotionDecision(blockId, executorId) =>
      localExecutorLockMap.putIfAbsent(executorId, new Object)
      val lock = localExecutorLockMap(executorId)
      lock.synchronized {
        context.reply(handlePromotionDecision(blockId, executorId))
      }

    case PromotionDone(blockId, stageId, executorId) =>
      BlazeLogger.promotionDone(blockId, stageId, executorId)

    case SendUnrollingTime(blockId, time) =>
      // Always maintain the latest unrolling time for this block
      metricTracker.blockIdToUnrollingTimeMap.put(blockId, time)

    case SendCompTime(blockId, key, time) =>
      if (metricTracker.parentBlockIdToBlockIdCompTimeMap.containsKey(key)) {
        metricTracker.parentBlockIdToBlockIdCompTimeMap.put(key,
          Math.max(metricTracker.parentBlockIdToBlockIdCompTimeMap.get(key), time))
      } else {
        metricTracker.parentBlockIdToBlockIdCompTimeMap.put(key, time)
      }
  }
}
