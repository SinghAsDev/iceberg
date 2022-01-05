/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.fileformat.read;

import java.util.function.Function;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class ReadBuilder<T> {
  private final InputFile file;
  private Schema schema = null;
  private Long start = null;
  private Long length = null;
  private Expression filter = null;
  private boolean caseSensitive = true;

  private Function<T, Reader<?>> readerFunc;
  private Function<T, Reader<?>> batchedReaderFunc;
  private int recordsPerBatch;

  private ReadBuilder(InputFile file) {
    Preconditions.checkNotNull(file, "Input file cannot be null");
    this.file = file;
  }

  /**
   * Restricts the read to the given range: [start, start + length).
   *
   * @param newStart  the start position for this read
   * @param newLength the length of the range this read should scan
   * @return this builder for method chaining
   */
  public ReadBuilder split(long newStart, long newLength) {
    this.start = newStart;
    this.length = newLength;
    return this;
  }

  public ReadBuilder project(Schema newSchema) {
    this.schema = newSchema;
    return this;
  }

  public ReadBuilder caseSensitive(boolean newCaseSensitive) {
    this.caseSensitive = newCaseSensitive;
    return this;
  }

  public ReadBuilder config(String property, String value) {
    // do something
    return this;
  }

  public ReadBuilder createReaderFunc(Function<T, Reader<?>> readerFunction) {
    Preconditions.checkArgument(
        this.batchedReaderFunc == null,
        "Reader function cannot be set since the batched version is already set");
    this.readerFunc = readerFunction;
    return this;
  }

  public ReadBuilder filter(Expression newFilter) {
    this.filter = newFilter;
    return this;
  }

  public ReadBuilder createBatchedReaderFunc(Function<T, Reader<?>> batchReaderFunction) {
    Preconditions.checkArgument(
        this.readerFunc == null,
        "Batched reader function cannot be set since the non-batched version is already set");
    this.batchedReaderFunc = batchReaderFunction;
    return this;
  }

  public ReadBuilder recordsPerBatch(int numRecordsPerBatch) {
    this.recordsPerBatch = numRecordsPerBatch;
    return this;
  }

  // public ReadBuilder withNameMapping(NameMapping newNameMapping) {
  //   this.nameMapping = newNameMapping;
  //   return this;
  // }

  public <D> CloseableIterable<D> build() {
    Preconditions.checkNotNull(schema, "Schema is required");
    return null;
    // return new OrcIterable<>(file, conf, schema, nameMapping, start, length, readerFunc, caseSensitive, filter,
    //     batchedReaderFunc, recordsPerBatch);
  }
}
