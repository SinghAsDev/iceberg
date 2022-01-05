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

package org.apache.iceberg.fileformat;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.fileformat.read.ReadBuilder;
import org.apache.iceberg.fileformat.write.DataWriteBuilder;
import org.apache.iceberg.fileformat.write.DeleteWriteBuilder;
import org.apache.iceberg.fileformat.write.WriteBuilder;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

public interface CustomFileFormat {
  default FileFormat getFileFormat() {
    return new FileFormat("", "", true);
  }
  String name();
  String ext();
  boolean isSplittable();
  String addExtension(String filename);

  WriteBuilder write(OutputFile file);

  DataWriteBuilder writeData(OutputFile file);

  DeleteWriteBuilder writeDeletes(OutputFile file);

  ReadBuilder read(InputFile file);

  long rowCount(InputFile file);

  void concat(
      Iterable<File> inputFiles, File outputFile, int rowGroupSize, Schema schema,
      Map<String, String> metadata) throws IOException;

  Metrics getMetrics(InputFile file);
}
