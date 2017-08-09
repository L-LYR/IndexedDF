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

package org.apache.spark.sql

import indexeddataframe.execution.IndexedOperatorExec
import indexeddataframe.logical.{AppendRows, CreateIndex, GetRows}
import org.apache.spark.sql.catalyst.InternalRow

import scala.collection.mutable.ArrayBuffer

class IndexedDatasetFunctions[T](ds: Dataset[T]) extends Serializable {
  def createIndex(colNo: Int): DataFrame = {
    Dataset.ofRows(ds.sparkSession, CreateIndex(colNo, ds.logicalPlan))
  }
  def appendRows(rows: Seq[InternalRow]): DataFrame = {
    Dataset.ofRows(ds.sparkSession, AppendRows(rows, ds.logicalPlan))
  }
  def getRows(key: Long): Array[InternalRow] = {
    //Dataset.ofRows(ds.sparkSession, GetRows(key, ds.logicalPlan))
    ds.queryExecution.executedPlan.asInstanceOf[IndexedOperatorExec].executeGetRows(key)
  }
  def multigetRows(keys: Array[Long]): Array[InternalRow] = {
    //Dataset.ofRows(ds.sparkSession, GetRows(key, ds.logicalPlan))
    ds.queryExecution.executedPlan.asInstanceOf[IndexedOperatorExec].executeMultiGetRows(keys)
  }
}