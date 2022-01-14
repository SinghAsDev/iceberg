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

import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.fileformat.example.schema.ThriftTypeToType;
import org.apache.iceberg.types.Type;
import org.apache.spark.sql.types.StructType;
import org.apache.thrift.TBase;

public class CustomSparkCatalog extends SparkCatalog {
  private static final String THRIFT_CLASS_KEY = "thrift_type";

  @Override
  protected Schema convert(StructType schema, Map<String, String> properties) {
    if (properties.containsKey(THRIFT_CLASS_KEY)) {
      String tClassStr = properties.get(THRIFT_CLASS_KEY);
      try {
        Class<?> tClass = Class.forName(tClassStr);
        Type converted = new ThriftTypeToType().convert((Class<? extends TBase<?, ?>>) tClass);
        return new Schema(converted.asNestedType().asStructType().fields());
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException(String.format("Thrift class %s not found.", tClassStr), e);
      }
    } else {
     return super.convert(schema, properties);
    }
  }
}
