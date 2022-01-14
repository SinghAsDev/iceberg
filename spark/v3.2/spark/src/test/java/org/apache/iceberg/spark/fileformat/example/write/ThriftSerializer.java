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

import java.util.List;
import org.apache.iceberg.spark.fileformat.example.schema.ThriftTypeVisitor;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.thrift.meta_data.StructMetaData;
import org.apache.thrift.protocol.TProtocol;

public class ThriftSerializer extends ThriftTypeVisitor<Void> {
  public static byte[] serialize(String tClass, InternalRow internalRow) {
    return new byte[0];
  }

  @Override
  public TProtocol struct(StructMetaData structMetaData, List<TProtocol> fieldResults) {

  }
}
