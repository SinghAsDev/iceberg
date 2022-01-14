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

package org.apache.iceberg.spark.fileformat.example.schema;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.EnumMetaData;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.ListMetaData;
import org.apache.thrift.meta_data.MapMetaData;
import org.apache.thrift.meta_data.SetMetaData;
import org.apache.thrift.meta_data.StructMetaData;
import org.apache.thrift.protocol.TType;

public class ThriftTypeVisitor<T> {
  static <T> T visit(FieldValueMetaData fieldValueMetaData, ThriftTypeVisitor<T> visitor) {
    switch (fieldValueMetaData.type) {
      case TType.STOP:
      case TType.VOID:
      default:
        throw new UnsupportedOperationException("Can't convert type " + fieldValueMetaData.type);
      case TType.BOOL:
      case TType.BYTE:
      case TType.DOUBLE:
      case TType.I16:
      case TType.I32:
      case TType.I64:
      case TType.STRING:
        return visitor.atomic(fieldValueMetaData);
      case TType.STRUCT:
        StructMetaData structMetaData = (StructMetaData) fieldValueMetaData;
        Map<? extends TFieldIdEnum, FieldMetaData> structMetaDataMap =
            FieldMetaData.getStructMetaDataMap(structMetaData.structClass);
        Map.Entry<? extends TFieldIdEnum, FieldMetaData>[] fields =
            structMetaDataMap.entrySet().toArray(new Map.Entry[structMetaDataMap.size()]);
        Arrays.sort(fields, Comparator.comparingInt(e -> e.getKey().getThriftFieldId()));

        List<T> fieldResults = Lists.newArrayListWithExpectedSize(fields.length);

        for (Map.Entry<? extends TFieldIdEnum, FieldMetaData> field : fields) {
          fieldResults.add(visitor.field(
              field.getKey(),
              field.getValue(),
              visit(field.getValue().valueMetaData, visitor)));
        }

        return visitor.struct(structMetaData, fieldResults);
      case TType.MAP:
        MapMetaData mapMetaData = (MapMetaData) fieldValueMetaData;

        return visitor.map(mapMetaData,
            visit(mapMetaData.keyMetaData, visitor),
            visit(mapMetaData.valueMetaData, visitor));
      case TType.SET:
        SetMetaData setMetaData = (SetMetaData) fieldValueMetaData;

        return visitor.set(setMetaData,
            visit(setMetaData.elemMetaData, visitor));
      case TType.LIST:
        ListMetaData listMetaData = (ListMetaData) fieldValueMetaData;

        return visitor.list(listMetaData,
            visit(listMetaData.elemMetaData, visitor));
      case TType.ENUM:
        EnumMetaData enumMetaData = (EnumMetaData) fieldValueMetaData;

        return visitor.enums(enumMetaData);
    }
  }

  public T struct(StructMetaData structMetaData, List<T> fieldResults) {
    return null;
  }

  public T field(TFieldIdEnum fieldIdEnum, FieldMetaData fieldMetaData, T typeResult) {
    return null;
  }

  public T list(ListMetaData listMetaData, T elementResult) {
    return null;
  }

  public T set(SetMetaData setMetaData, T elementType) {
    return null;
  }

  public T map(MapMetaData mapMetaData, T keyResult, T valueResult) {
    return null;
  }

  public T atomic(FieldValueMetaData fieldMetaData) {
    return null;
  }

  public T enums(EnumMetaData enumMetaData) {
    return null;
  }
}
