/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.spark.fileformat.example;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.fileformat.CustomFileFormat;
import org.apache.iceberg.fileformat.read.ReadBuilder;
import org.apache.iceberg.fileformat.write.DataWriteBuilder;
import org.apache.iceberg.fileformat.write.DeleteWriteBuilder;
import org.apache.iceberg.fileformat.write.WriteBuilder;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.spark.fileformat.example.write.ThriftSequenceFileDataWriteBuilder;

public class ThriftSequenceFileFormat implements CustomFileFormat {
  @Override
  public String name() {
    return "THRIFT_SEQUENCEFILE";
  }

  @Override
  public String ext() {
    return "thrift_seq";
  }

  @Override
  public boolean isSplittable() {
    return true;
  }

  @Override
  public String addExtension(String filename) {
    return null;
  }

  @Override
  public WriteBuilder write(OutputFile file) {
    return null;
  }

  @Override
  public DataWriteBuilder writeData(OutputFile file) {
    return new ThriftSequenceFileDataWriteBuilder(file, this);
  }

  @Override
  public DeleteWriteBuilder writeDeletes(OutputFile file) {
    return null;
  }

  @Override
  public <T> ReadBuilder<T> read(InputFile file) {
    return null;
  }

  @Override
  public long rowCount(InputFile file) {
    return 0;
  }

  @Override
  public void concat(
      Iterable<File> inputFiles, File outputFile, int rowGroupSize, Schema schema, Map<String, String> metadata)
      throws IOException {

  }

  @Override
  public Metrics getMetrics(InputFile file) {
    return null;
  }
}
