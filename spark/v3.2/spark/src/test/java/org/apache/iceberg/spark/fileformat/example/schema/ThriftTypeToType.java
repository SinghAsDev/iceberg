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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.TFieldRequirementType;
import org.apache.thrift.meta_data.EnumMetaData;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.ListMetaData;
import org.apache.thrift.meta_data.MapMetaData;
import org.apache.thrift.meta_data.SetMetaData;
import org.apache.thrift.meta_data.StructMetaData;
import org.apache.thrift.protocol.TType;

public class ThriftTypeToType extends ThriftTypeVisitor<Type> {

  AtomicInteger id = new AtomicInteger(0);

  public Type convert(Class<? extends TBase<?, ?>> tClass) {
    return visit(new StructMetaData(TType.STRUCT, tClass), this);
  }

  @Override
  public Type struct(StructMetaData structMetaData, List<Type> fieldResults) {
    // TODO: don't sort twice, use a struct to pass sorted struct metadata instead
    Map<? extends TFieldIdEnum, FieldMetaData> structMetaDataMap =
        FieldMetaData.getStructMetaDataMap(structMetaData.structClass);
    Map.Entry<? extends TFieldIdEnum, FieldMetaData>[] fields =
        structMetaDataMap.entrySet().toArray(new Map.Entry[structMetaDataMap.size()]);
    Arrays.sort(fields, Comparator.comparingInt(e -> e.getKey().getThriftFieldId()));

    List<Types.NestedField> newFields = Lists.newArrayListWithExpectedSize(fields.length);
    for (int i = 0; i < fields.length; ++i) {
      Map.Entry<? extends TFieldIdEnum, FieldMetaData> field = fields[i];
      Type type = fieldResults.get(i);

      if (field.getValue().requirementType == TFieldRequirementType.REQUIRED) {
        newFields.add(Types.NestedField.required(
            id.incrementAndGet(), field.getKey().getFieldName(), type));
      } else {
        newFields.add(Types.NestedField.optional(
            id.incrementAndGet(), field.getKey().getFieldName(), type));
      }
    }

    return Types.StructType.of(newFields);
  }

  @Override
  public Type field(TFieldIdEnum fieldIdEnum, FieldMetaData fieldMetaData, Type typeResult) {
    return typeResult;
  }

  @Override
  public Type list(ListMetaData listMetaData, Type elementResult) {
    return Types.ListType.ofOptional(id.incrementAndGet(), elementResult);
  }

  @Override
  public Type set(SetMetaData setMetaData, Type elementType) {
    return Types.ListType.ofOptional(id.incrementAndGet(), elementType);
  }

  @Override
  public Type map(MapMetaData mapMetaData, Type keyResult, Type valueResult) {
      return Types.MapType.ofOptional(id.incrementAndGet(), id.incrementAndGet(), keyResult, valueResult);
  }

  @Override
  public Type atomic(FieldValueMetaData fieldMetaData) {
    switch (fieldMetaData.type) {
      case TType.STOP:
      case TType.VOID:
      default:
        throw new UnsupportedOperationException("Not supported type: " + fieldMetaData.type);
      case TType.BOOL:
        return Types.BooleanType.get();
      case TType.BYTE:
      case TType.I16:
      case TType.I32:
        return Types.IntegerType.get();
      case TType.I64:
        return Types.LongType.get();
      case TType.DOUBLE:
        return Types.DoubleType.get();
      case TType.STRING:
        return Types.StringType.get();
    }
  }

  @Override
  public Type enums(EnumMetaData enumMetaData) {
    return Types.StringType.get();
  }
}
