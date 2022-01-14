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

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.avro.io.DatumWriter;
import org.apache.iceberg.IMetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.fileformat.CustomFileFormat;
import org.apache.iceberg.fileformat.write.DataWriteBuilder;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.IDataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class ThriftSequenceFileDataWriteBuilder implements DataWriteBuilder {
  private final OutputFile file;
  private final String location;
  private final CustomFileFormat fileFormat;

  private Schema schema;
  private boolean overwrite;

  private Map<String, String> keyValueMetadata = Maps.newHashMap();
  private Map<String, String> config = Maps.newHashMap();
  private IMetricsConfig metricsConfig = null;
  private PartitionSpec spec = null;
  private StructLike partition = null;
  private EncryptionKeyMetadata encryptionKeyMetadata = null;
  private SortOrder sortOrder = null;
  private BiFunction rowWriter = null;

  public ThriftSequenceFileDataWriteBuilder(OutputFile file, CustomFileFormat fileFormat) {
    this.file = file;
    this.location = file.location();
    this.fileFormat = fileFormat;
  }


  @Override
  public DataWriteBuilder forTable(Table table) {
    return null;
  }

  @Override
  public DataWriteBuilder schema(Schema newSchema) {
    this.schema = newSchema;
    return this;
  }

  @Override
  public DataWriteBuilder set(String property, String value) {
    this.config.put(property, value);
    return this;
  }

  @Override
  public DataWriteBuilder setAll(Map<String, String> properties) {
    this.config.putAll(properties);
    return this;
  }

  @Override
  public DataWriteBuilder meta(String property, String value) {
    this.keyValueMetadata.put(property, value);
    return this;
  }

  @Override
  public DataWriteBuilder overwrite() {
    this.overwrite = true;
    return this;
  }

  @Override
  public DataWriteBuilder overwrite(boolean enabled) {
    this.overwrite = enabled;
    return this;
  }

  @Override
  public DataWriteBuilder metricsConfig(IMetricsConfig newMetricsConfig) {
    this.metricsConfig = newMetricsConfig;
    return this;
  }

  @Override
  public DataWriteBuilder createWriterFunc(BiFunction writerFunction) {
    this.rowWriter = writerFunction;
    return this;
  }

  @Override
  public DataWriteBuilder withSpec(PartitionSpec newSpec) {
    this.spec = newSpec;
    return this;
  }

  @Override
  public DataWriteBuilder withPartition(StructLike newPartition) {
    this.partition = newPartition;
    return this;
  }

  @Override
  public DataWriteBuilder withKeyMetadata(EncryptionKeyMetadata metadata) {
    this.encryptionKeyMetadata = metadata;
    return this;
  }

  @Override
  public DataWriteBuilder withSortOrder(SortOrder newSortOrder) {
    this.sortOrder = newSortOrder;
    return this;
  }

  @Override
  public <T> IDataWriter<T> build() {
    Preconditions.checkArgument(spec != null, "Cannot create data writer without spec");
    Preconditions.checkArgument(spec.isUnpartitioned() || partition != null,
        "Partition must not be null when creating data writer for partitioned spec");

    FileAppender<T> fileAppender = new ThriftSequenceFileAppender<>(schema, file, config, metricsConfig,
        overwrite, rowWriter);
    return new DataWriter<>(fileAppender, fileFormat.getFileFormat(), location, spec, partition, encryptionKeyMetadata,
        sortOrder);
  }
}
