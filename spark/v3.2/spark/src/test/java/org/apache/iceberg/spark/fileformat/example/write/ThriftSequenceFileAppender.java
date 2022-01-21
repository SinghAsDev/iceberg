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

package org.apache.iceberg.spark.fileformat.example.write;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.iceberg.IMetricsConfig;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.spark.fileformat.example.schema.SparkNativeThriftSerDe;
import org.apache.iceberg.spark.fileformat.example.schema.ThriftSerDeOptions;
import org.apache.spark.sql.catalyst.InternalRow;

public class ThriftSequenceFileAppender<T> implements FileAppender<T>, Closeable {
  private static final String THRIFT_CLASS_KEY = "thrift_type";

  private PositionOutputStream stream = null;
  private org.apache.iceberg.Schema icebergSchema;
  private IMetricsConfig metricsConfig;
  private SequenceFile.Writer writer;
  private BiFunction rowWriter;
  private Map<String, String> properties;
  private String tClass;
  private long numRecords = 0L;

  public ThriftSequenceFileAppender(
      Schema icebergSchema, OutputFile file, Map<String, String> properties,
      IMetricsConfig metricsConfig, boolean overwrite, BiFunction rowWriter) {
    this.icebergSchema = icebergSchema;
    this.stream = overwrite ? file.createOrOverwrite() : file.create();
    this.metricsConfig = metricsConfig;
    this.properties = properties;
    this.tClass = properties.get(THRIFT_CLASS_KEY);
    try {
      Path path;
      Configuration conf;
      if (file instanceof HadoopOutputFile) {
        conf = ((HadoopOutputFile) file).getConf();
        path = ((HadoopOutputFile) file).getPath();
      } else {
        conf = new Configuration();
        path = new Path(file.location());
      }

      this.writer = SequenceFile.createWriter(conf,
          SequenceFile.Writer.file(path),
          // SequenceFile.Writer.stream(new FSDataOutputStream(stream, null)), // todo: not flushed for small data
          SequenceFile.Writer.keyClass(NullWritable.class),
          SequenceFile.Writer.valueClass(BytesWritable.class),
          SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE),
          SequenceFile.Writer.appendIfExists(false)
          );
    } catch (IOException e) {
      e.printStackTrace();
    }
    this.rowWriter = rowWriter;
  }

  @Override
  public void add(T datum) {
    byte[] bytes;
    if (datum instanceof InternalRow) {
      //bytes = "dqwfwffasfsdfdsfsfawfwfwefwefwefwefqwF3FASFDASwe, fqwfwefewfwe".getBytes(StandardCharsets.UTF_8);
      SparkNativeThriftSerDe serde = new SparkNativeThriftSerDe();
      serde.initialize(
          tClass,
          new ThriftSerDeOptions(false, true, false, false),
          new Properties());

      bytes = serde.serialize((InternalRow) datum, false);
    } else {
      throw new UnsupportedOperationException(String.format("Writing data of type %s is not supported by %s.",
          datum.getClass().getName(), this.getClass().getName()));
    }
    numRecords += 1L;
    try {
      writer.append(NullWritable.get(), new BytesWritable(bytes));
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write to SequenceFile.", e);
    }
  }

  @Override
  public Metrics metrics() {
    return new Metrics(numRecords, null, null, null, null);
  }

  @Override
  public long length() {
    if (stream != null) {
      try {
        return stream.getPos();
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to get stream length", e);
      }
    }
    throw new RuntimeIOException("Failed to get stream length: no open stream");
  }

  @Override
  public void close() throws IOException {
    if (writer != null) {
      writer.close();
      this.writer = null;
    }
  }
}
