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
import org.apache.spark.internal.config.ConfigBuilder

private[spark] object BlazeParameters extends Logging {

  // TODO: Remove everything but 'testable knobs'

  val readThp = 3000.0 / (600 * 1024 * 1024)
  val writeThp = 3000.0 / (600 * 1024 * 1024)

  /* Sampling */

  private[spark] val DAG_PATH = ConfigBuilder("spark.blaze.dagPath")
    .stringConf
    .createWithDefault("None")

  private[spark] val IS_PROFILE_RUN = ConfigBuilder("spark.blaze.isProfileRun")
    .booleanConf
    .createWithDefault(false)

  private[spark] val PROFILE_NUM_STAGES = ConfigBuilder("spark.blaze.profileNumStages")
    .intConf
    .createWithDefault(Int.MaxValue)

  private[spark] val PROFILE_NUM_JOBS = ConfigBuilder("spark.blaze.profileNumJobs")
    .intConf
    .createWithDefault(Int.MaxValue)

  /* Policies */

  private[spark] val AUTOCACHING = ConfigBuilder("spark.blaze.autoCaching")
    .booleanConf
    .createWithDefault(false)

  private[spark] val LAZY_AUTOCACHING = ConfigBuilder("spark.blaze.lazyAutoCaching")
    .booleanConf
    .createWithDefault(false)

  private[spark] val AUTOUNPERSIST = ConfigBuilder("spark.blaze.autoUnpersist")
    .booleanConf
    .createWithDefault(false)

  private[spark] val COST_FUNCTION = ConfigBuilder("spark.blaze.costFunction")
    .stringConf
    .createWithDefault("None")

  private[spark] val ENABLE_DISKSTORE = ConfigBuilder("spark.blaze.enableDiskStore")
    .booleanConf
    .createWithDefault(true)
}
