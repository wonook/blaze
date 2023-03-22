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
 * Deprecated.
 *
 * Caching policy manages which RDDs are marked as cached.
 * Note that this policy maintains RDD-level info only, not block-level ones.
 * Thus, this policy is only used for checking whether an RDD is marked as cached,
 * not for actual state management of memoized blocks.
 */
private[spark] class BlazeCachingPolicy()
  extends CachingPolicy with Logging {

  private val userCachedRDDs = new mutable.HashSet[Int]()
  private val blazeCachedRDDs = new mutable.HashSet[Int]()

  def addUserCachedRDD(rddId: Int): Unit = {
    userCachedRDDs.add(rddId)
  }

  def addBlazeCachedRDD(rddId: Int): Unit = {
    blazeCachedRDDs.add(rddId)
  }

  def isUserCachedRDD(rddId: Int): Boolean = {
    userCachedRDDs.contains(rddId)
  }

  def getUserCachedRDD(): mutable.Set[Int] = {
    userCachedRDDs
  }

  def isBlazeCachedRDD(rddId: Int): Boolean = {
    blazeCachedRDDs.contains(rddId)
  }

  def isRDDCached(rddId: Int): Boolean = {
    userCachedRDDs.contains(rddId) || blazeCachedRDDs.contains(rddId)
  }
}


