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

import com.google.common.collect.ImmutableMap;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.fileformat.CustomFileFormat;
import org.apache.iceberg.fileformat.FileFormatFactory;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.util.SerializableSupplier;

public class ExampleFileFormatFactory implements FileFormatFactory, HadoopConfigurable {

  private static final Map<String, String> fileFormats = ImmutableMap.<String, String> builder()
      .put("thrift_sequencefile", "org.apache.iceberg.spark.fileformat.example.ThriftSequenceFileFormat")
      .build();
  private SerializableSupplier<Configuration> hadoopConf;

  @Override
  public void initialize(Map<String, String> properties) {
  }

  @Override
  public CustomFileFormat get(String name) {
    if (fileFormats.containsKey(name)) {
      try {
        DynConstructors.Ctor<CustomFileFormat> ctor = DynConstructors.builder(CustomFileFormat.class)
            .hiddenImpl(fileFormats.get(name)).buildChecked();
        CustomFileFormat customFileFormat = ctor.newInstance();
        return customFileFormat;
      } catch (NoSuchMethodException e) {
        e.printStackTrace();
      }
    }
    return null;
  }

  @Override
  public Collection<CustomFileFormat> getAll() {
    return null;
  }

  @Override
  public void serializeConfWith(Function<Configuration, SerializableSupplier<Configuration>> confSerializer) {
    this.hadoopConf = confSerializer.apply(getConf());
  }

  @Override
  public void setConf(Configuration conf) {
    this.hadoopConf = new SerializableConfiguration(conf)::get;
  }

  @Override
  public Configuration getConf() {
    return hadoopConf.get();
  }
}
