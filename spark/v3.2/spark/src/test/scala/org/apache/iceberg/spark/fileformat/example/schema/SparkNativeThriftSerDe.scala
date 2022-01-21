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

package org.apache.iceberg.spark.fileformat.example.schema

import java.io.UnsupportedEncodingException
import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type
import java.nio.ByteBuffer
import java.util
import java.util.Properties
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.MapType
import org.apache.spark.sql.types.NullType
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.apache.thrift.TBase
import org.apache.thrift.TBaseHelper
import org.apache.thrift.TEnum
import org.apache.thrift.TException
import org.apache.thrift.TFieldIdEnum
import org.apache.thrift.TUnion
import org.apache.thrift.meta_data.EnumMetaData
import org.apache.thrift.meta_data.FieldMetaData
import org.apache.thrift.meta_data.FieldValueMetaData
import org.apache.thrift.meta_data.ListMetaData
import org.apache.thrift.meta_data.MapMetaData
import org.apache.thrift.meta_data.SetMetaData
import org.apache.thrift.meta_data.StructMetaData
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.protocol.TCompactProtocol
import org.apache.thrift.protocol.TField
import org.apache.thrift.protocol.TList
import org.apache.thrift.protocol.TMap
import org.apache.thrift.protocol.TProtocol
import org.apache.thrift.protocol.TProtocolUtil
import org.apache.thrift.protocol.TSet
import org.apache.thrift.protocol.TStruct
import org.apache.thrift.protocol.TType
import org.apache.thrift.transport.TMemoryBuffer
import org.apache.thrift.transport.TMemoryInputTransport

import scala.collection.JavaConverters.mapAsScalaMapConverter

class SparkNativeThriftSerDe() {
  private val fieldsToDeserializeKey = "fields_to_deserialize"
  private val inTransport = new TMemoryInputTransport()
  private var thriftSchema: SparkNativeThriftSerDe.ThriftStructType = null

  /**
   * Initialize SparkNativeThriftSerDe. Its during this initialization, when all
   * metadata needed to deserialize serialized thrift objects is constructed.
   *
   * @param tClass             thrift class whose serialized instance needs to be deserialized
   * @param thriftSerdeOptions deserialization options to control deserialization behavior
   * @param properties         table properties, passed down from Hive's
   */
  def initialize(tClassStr: String,
                 thriftSerdeOptions: ThriftSerDeOptions,
                 properties: Properties): Unit = {
    val tClass = Class.forName(tClassStr).asInstanceOf[Class[_ <: TBase[T,F]
        forSome { type T <: TBase[T,F]; type F <: TFieldIdEnum }]]
    thriftSchema = SparkNativeThriftSerDe.toSchema(
      new StructMetaData(TType.STRUCT, tClass),
      SparkNativeThriftSerDe.getFieldsToDeserialize(
        properties.getProperty(fieldsToDeserializeKey, "")),
      thriftSerdeOptions, tClass).asInstanceOf[SparkNativeThriftSerDe.ThriftStructType]
  }

  /**
   * Deserialize given input array into a thrift object of type provided during initialization.
   *
   * @param bytes input byte array representing a serialized thrift object
   * @return Object of type Spark's {@link org.apache.spark.sql.catalyst.InternalRow}
   */
  def deserialize(bytes: Array[Byte]): AnyRef = deserialize(bytes, 0, bytes.length, false)

  /**
   * Deserialize given input array into a thrift object of type provided during initialization.
   *
   * @param bytes              input byte array representing a serialized thrift object
   * @param offset             offset in input byte array from where to start reading bytes to be deserialized
   * @param length             number of bytes to be read from offset for deserialization
   * @param useCompactProtocol if {@link org.apache.thrift.protocol.TCompactProtocol} needs to be used.
   *                           If false, {@link org.apache.thrift.protocol.TBinaryProtocol} is used.
   * @return Object of type Spark's {@link org.apache.spark.sql.catalyst.InternalRow}
   */
  def deserialize(bytes: Array[Byte],
                  offset: Int,
                  length: Int,
                  useCompactProtocol: Boolean = false): AnyRef = {
    inTransport.reset(bytes, offset, length)
    thriftSchema.read(if (useCompactProtocol) new TCompactProtocol(inTransport) else new TBinaryProtocol(inTransport))
  }

  def serialize(internalRow: InternalRow,
                useCompactProtocol: Boolean = false): Array[Byte] = {
    val outTransport = new TMemoryBuffer(32*2) // todo: reuse _arr from TMemoryBuffer
    // outTransport.reset()
    thriftSchema.write(
      if (useCompactProtocol) new TCompactProtocol(outTransport) else new TBinaryProtocol(outTransport),
      internalRow, None)
    outTransport.getArray
  }
}

private object SparkNativeThriftSerDe {

  /**
   * A structure to hold a thrift field along with all its children.
   *
   * @param name name of this thrift field
   */
  class ThriftField(val name: String) {
    private var fields: Set[ThriftField] = Set.empty

    /**
     * Get all children of this thrift field
     *
     * @return all children of this thrift field
     */
    def children: Set[ThriftField] = fields

    /**
     * Add a child to this thrift field. This will recursively create child
     * of the added children if needed.
     *
     * @param child array of names representing the lineage, e.g., ["first", "second", "third"]
     *              will be added as current.first.second.third
     */
    def add(child: Array[String]): Unit = {
      if (child.nonEmpty) {
        fields.find(_.name.equals(child.head)) match {
          case Some(f: ThriftField) => f.add(child.slice(1, child.length))
          case None =>
            val tf = new ThriftField(child.head)
            tf.add(child.slice(1, child.length))
            fields = fields + tf
        }
      }
    }
  }

  /**
   * Constructs a tree of fields needed to be deserialized
   *
   * @param fieldsString string representing comma delimited fields to be deserialized
   * @return a set of thrift fields representing the tree of thrift fields needed to be deserialized
   */
  def getFieldsToDeserialize(fieldsString: String): Set[ThriftField] = {
    var fields: Set[ThriftField] = Set.empty

    def add(fieldFqdn: String): Unit = {
      val parts = fieldFqdn.split('.')
      val root = parts.head
      val children = parts.slice(1, parts.length)
      fields.find(f => f.name.equals(root)) match {
        case Some(f: ThriftField) => f.add(children)
        case None =>
          val tf = new ThriftField(root)
          tf.add(children)
          fields = fields + tf
      }
    }

    fieldsString.split(',').map(_.trim).foreach(add)
    fields
  }

  /**
   * Convert the field metadata to the associated Spark SQL type. Binary is handled
   * separately because thrift does not differentiate between binary blobs and string
   * in its field metadata.
   *
   * @param field               the field metadata provided by thrift
   * @param fieldsToDeserialize list of fields to deserialize
   * @param thriftSerDeOptions  thrift serde options
   * @param fieldType           field type for the field from thrift's class
   * @return {@link ThriftDataType} capable of deserializing a serialized thrift object of
   *         type `field` into {@link org.apache.spark.sql.catalyst.InternalRow}.
   */
  def toSchema(field: FieldValueMetaData,
               fieldsToDeserialize: Set[ThriftField] = Set.empty,
               thriftSerDeOptions: ThriftSerDeOptions = new ThriftSerDeOptions(),
               fieldType: Type): ThriftDataType = {
    val (isBinary, isBitSet) = fieldType match {
      case fieldClass: Class[_] =>
        (classOf[java.nio.ByteBuffer].isAssignableFrom(fieldClass),
          classOf[util.BitSet].isAssignableFrom(fieldClass))
      case _ => (false, false)
    }
    val nullIfNotSet: Boolean = thriftSerDeOptions.nullIfNotSet

    (field.`type`, field) match {
      case (TType.BOOL, _) => new ThriftBooleanType
      case (TType.BYTE, _) => new ThriftByteType
      case (TType.I16, _) => new ThriftShortType
      case (TType.I32, _) => new ThriftIntegerType
      case (TType.I64, _) => new ThriftLongType
      case (TType.DOUBLE, _) => new ThriftDoubleType
      case (TType.STRING, _) if (isBinary && thriftSerDeOptions.convertByteBufferToString) =>
        new ThriftByteBufferAsStringType
      case (TType.STRING, _) if isBinary && thriftSerDeOptions.convertByteBufferToBytes =>
        new ThriftBinaryType
      case (TType.STRING, _) if isBinary => new ThriftByteBufferType
      case (TType.STRING, _) if isBitSet => new ThriftBitSetType
      case (TType.STRING, _) => new ThriftStringType
      case (TType.ENUM, enumMetaData: EnumMetaData) if thriftSerDeOptions.convertEnumToString =>
        val enumToString = enumMetaData.enumClass.getDeclaredMethod("values")
          .invoke(null).asInstanceOf[Array[TEnum]]
          .map { (v: TEnum) => (v.getValue, UTF8String.fromString(v.toString)) }.toMap
        new ThriftEnumAsStringType(enumToString)
      case (TType.ENUM, _) => new ThriftEnumType
      case (TType.LIST, listMetaData: ListMetaData) =>
        val pt = fieldType.asInstanceOf[ParameterizedType]
        val typeArgs = pt.getActualTypeArguments.toList
        val elementType = typeArgs.head
        val elementDataType = toSchema(listMetaData.elemMetaData, fieldsToDeserialize, thriftSerDeOptions, elementType)
        if (elementDataType.dataType.typeName.equals(NullType.typeName)) {
          new ThriftNullType(TType.LIST)
        } else {
          new ThriftListType(listMetaData.elemMetaData.`type`, elementDataType)
        }
      case (TType.SET, setMetaData: SetMetaData) =>
        val pt = fieldType.asInstanceOf[ParameterizedType]
        val typeArgs = pt.getActualTypeArguments.toList
        val elementType = typeArgs.head
        val elementDataType = toSchema(setMetaData.elemMetaData, fieldsToDeserialize, thriftSerDeOptions, elementType)
        if (elementDataType.dataType.typeName.equals(NullType.typeName)) {
          new ThriftNullType(TType.SET)
        } else {
          new ThriftSetType(setMetaData.elemMetaData.`type`, elementDataType)
        }
      //TODO: add fieldsToDeserialize handling to the map type
      case (TType.MAP, mapMetaData: MapMetaData) =>
        val pt = fieldType.asInstanceOf[ParameterizedType]
        val typeArgs = pt.getActualTypeArguments.toList
        val keyType = typeArgs.head
        val valueType = typeArgs.last
        val keyDataType = toSchema(mapMetaData.keyMetaData, Set.empty, thriftSerDeOptions, keyType)
        val valueDataType = toSchema(mapMetaData.valueMetaData, Set.empty, thriftSerDeOptions, valueType)
        if (keyDataType.dataType.typeName.equals(NullType.typeName)
          || valueDataType.dataType.typeName.equals(NullType.typeName)) {
          new ThriftNullType(TType.MAP)
        } else {
          new ThriftMapType(mapMetaData.keyMetaData.`type`, keyDataType,
            mapMetaData.valueMetaData.`type`, valueDataType)
        }
      case (TType.STRUCT, structMetaData: StructMetaData) =>
        val structClass = structMetaData.structClass
        if (classOf[TUnion[_, _]].isAssignableFrom(structClass)) {
          new ThriftNullType(TType.STRUCT)
        } else {
          // TODO: reuse struct defs
          val structDefInstance = structClass.newInstance().asInstanceOf[TBase[_, TFieldIdEnum]]
          val fields = FieldMetaData.getStructMetaDataMap(structClass).asScala.toList
            .flatMap {
              case (structField, metaData) if structClass.getDeclaredFields.exists(
                f => f.getName.equals(metaData.fieldName)) =>
                val thriftFieldType = structClass.getDeclaredField(metaData.fieldName).getGenericType
                val isBinary = if (thriftFieldType.isInstanceOf[ParameterizedType]) {
                  false
                } else {
                  classOf[java.nio.ByteBuffer].isAssignableFrom(thriftFieldType.asInstanceOf[Class[_]])
                }
                val childFieldsToDeserialize: Set[ThriftField] = fieldsToDeserialize.find(
                  _.name.toLowerCase.equals(metaData.fieldName.toLowerCase)) match {
                  case Some(f) => f.children
                  case None => Set.empty
                }
                val fieldType = toSchema(metaData.valueMetaData, childFieldsToDeserialize,
                  thriftSerDeOptions, thriftFieldType)
                val deserializeField = fieldsToDeserialize.isEmpty ||
                  (fieldsToDeserialize.nonEmpty && fieldsToDeserialize.exists(
                    _.name.toLowerCase.equals(metaData.fieldName.toLowerCase)))
                val defaultValue = if (!deserializeField || metaData.valueMetaData.isContainer || isBinary) {
                  null
                } else {
                  val fieldValue = structDefInstance.getFieldValue(structField)
                  fieldValue match {
                    case fv if fv.isInstanceOf[String] => UTF8String.fromString(fv.asInstanceOf[String])
                    case fv if fv.isInstanceOf[TEnum] && thriftSerDeOptions.convertEnumToString =>
                      UTF8String.fromString(fv.asInstanceOf[TEnum].toString)
                    case fv if fv.isInstanceOf[TEnum] =>
                      new GenericInternalRow(Array[Any](fv.asInstanceOf[TEnum].getValue))
                    case fv => fv
                  }
                }
                Some(new ThriftStructField(structField.getThriftFieldId,
                  metaData.valueMetaData.`type`, metaData.fieldName, fieldType,
                  defaultValue, deserializeField))
              case (_, _) => None
            }
          // TODO: add support for passing bitsetvector info
          //        val updatedFields = if (!ignoreIssetBitVector) {
          //          new ThriftStructField((fields.last.id + 1).toShort, TType.STRING, "__isset_bit_vector",
          //            new ThriftBitSetType, null, true) :: fields
          //        } else fields
          new ThriftStructType(new TStruct(structClass.getName), fields, nullIfNotSet)
        }
      case _ => throw new IllegalArgumentException(
        s"Unknown thrift type detected: ${field.`type`}.")
    }
  }

  /**
   * This is a copy of intToZigZag from {@link org.apache.thrift.protocol.TCompactProtocol}, which is
   * not accessible from here.
   */
  def intToZigZag(n: Int): Int = n << 1 ^ n >> 31

  /**
   * Container for an abstract thrift data type to Spark SQL's type conversion
   *
   * @param dataType the associated Spark SQL data type to this thrift data type
   */
  abstract sealed case class ThriftDataType(dataType: DataType) extends Serializable {

    /**
     * Consume from the thrift protocol and processes to the appropriate type based on the schema
     *
     * @param proto the thrift protocol to consume from
     * @return the value from the protocol casted to the appropriate type
     */
    def read(proto: TProtocol): Any

    def write(proto: TProtocol, data: Any, index: Option[Int]): Unit
  }

  /**
   * Convert a thrift string to a Spark SQL's UTF8String by directly consuming from transport buffer
   */
  class ThriftStringType extends ThriftDataType(StringType) {
    override def read(proto: TProtocol): Any = {
      proto match {
        case proto if proto.isInstanceOf[TBinaryProtocol] =>
          val size = proto.readI32
          if (proto.getTransport.getBytesRemainingInBuffer >= size) {
            try {
              val str = UTF8String.fromBytes(proto.getTransport.getBuffer, proto.getTransport.getBufferPosition, size)
              proto.getTransport.consumeBuffer(size)
              str
            } catch {
              case _: UnsupportedEncodingException =>
                throw new TException("JVM DOES NOT SUPPORT UTF-8")
              case t: Throwable =>
                throw t
            }
          } else {
            try {
              val buf = new Array[Byte](size)
              proto.getTransport.readAll(buf, 0, size)
              UTF8String.fromBytes(buf, 0, size)
            } catch {
              case _: UnsupportedEncodingException =>
                throw new TException("JVM DOES NOT SUPPORT UTF-8")
            }
          }
        case proto if proto.isInstanceOf[TCompactProtocol] =>
          val length = intToZigZag(proto.readI32)
          if (length == 0) {
            UTF8String.EMPTY_UTF8
          } else {
            try {
              if (proto.getTransport.getBytesRemainingInBuffer >= length) {
                val str = UTF8String.fromBytes(
                  proto.getTransport.getBuffer, proto.getTransport.getBufferPosition, length)
                proto.getTransport.consumeBuffer(length)
                str
              } else {
                if (length == 0) {
                  Array.emptyByteArray
                } else {
                  val buf = new Array[Byte](length)
                  proto.getTransport.readAll(buf, 0, length)
                  UTF8String.fromBytes(buf, 0, length)
                }
              }
            }
            catch {
              case _: UnsupportedEncodingException =>
                throw new TException("UTF-8 not supported!")
            }
          }
      }
    }

    override def write(proto: TProtocol, data: Any, index: Option[Int]): Unit = {
      val str = index match {
        case Some(i) if data.isInstanceOf[InternalRow] => data.asInstanceOf[InternalRow].getString(i)
        case _ => data.asInstanceOf[UTF8String].toString
      }

      proto.writeString(str)
    }
  }

  /**
   * Convert a thrift binary blob to a bitset
   */
  class ThriftBitSetType extends ThriftDataType(BinaryType) {
    override def read(proto: TProtocol): Any = util.BitSet.valueOf(proto.readBinary).toByteArray

    override def write(proto: TProtocol, data: Any, index: Option[Int]): Unit = {
      val bytes = index match {
        case Some(i) if data.isInstanceOf[InternalRow] => data.asInstanceOf[InternalRow].getBinary(i)
        case _ => data.asInstanceOf[Array[Byte]]
      }

      proto.writeBinary(ByteBuffer.wrap(bytes))
    }
  }

  /**
   * Convert a thrift binary blob to a binary byte array type
   */
  class ThriftBinaryType extends ThriftDataType(BinaryType) {
    override def read(proto: TProtocol): Any = TBaseHelper.rightSize(proto.readBinary).array()

    override def write(proto: TProtocol, data: Any, index: Option[Int]): Unit = {
      val bytes = index match {
        case Some(i) if data.isInstanceOf[InternalRow] => data.asInstanceOf[InternalRow].getBinary(i)
        case _ => data.asInstanceOf[Array[Byte]]
      }

      proto.writeBinary(ByteBuffer.wrap(bytes))
    }
  }

  /**
   * Convert a thrift binary blob to a serialized byte buffer type
   */
  class ThriftByteBufferType extends
    ThriftDataType(StructType(Array(
      StructField("hb", BinaryType),
      StructField("offset", IntegerType),
      StructField("isreadonly", BooleanType),
      StructField("bigendian", BooleanType),
      StructField("nativebyteorder", BooleanType)))) {
    override def read(proto: TProtocol): Any =
      new GenericInternalRow(Array[Any](TBaseHelper.rightSize(proto.readBinary).array(),
        null, null, null, null))

    override def write(proto: TProtocol, data: Any, index: Option[Int]): Unit = {
      val bytes = data match {
        case d: InternalRow => d.getBinary(0)
        case _ => throw new IllegalArgumentException(
          s"Expecting Struct but got something else.")
      }

      proto.writeBinary(ByteBuffer.wrap(bytes))
    }
  }

  /**
   * Convert a thrift binary blob to a byte buffer as string
   */
  class ThriftByteBufferAsStringType extends ThriftDataType(StringType) {
    override def read(proto: TProtocol): Any =
      UTF8String.fromBytes(TBaseHelper.rightSize(proto.readBinary).array())

    override def write(proto: TProtocol, data: Any, index: Option[Int]): Unit = {
      val bytes = index match {
        case Some(i) if data.isInstanceOf[InternalRow] => data.asInstanceOf[InternalRow].getUTF8String(i).getBytes
        case _ => data.asInstanceOf[UTF8String].getBytes
      }

      proto.writeBinary(ByteBuffer.wrap(bytes))
    }
  }

  /**
   * Convert a thrift boolean to a Spark SQL boolean
   */
  class ThriftBooleanType extends ThriftDataType(BooleanType) {
    override def read(proto: TProtocol): Any = proto.readBool

    override def write(proto: TProtocol, data: Any, index: Option[Int]): Unit = {
      proto.writeBool(data.asInstanceOf[Boolean])
    }
  }

  /**
   * Convert a thrift byte to a Spark SQL byte
   */
  class ThriftByteType extends ThriftDataType(ByteType) {
    override def read(proto: TProtocol): Any = proto.readByte

    override def write(proto: TProtocol, data: Any, index: Option[Int]): Unit = {
      proto.writeByte(data.asInstanceOf[Byte])
    }
  }

  /**
   * Convert a thrift short to a Spark SQL short
   */
  class ThriftShortType extends ThriftDataType(ShortType) {
    override def read(proto: TProtocol): Any = proto.readI16

    override def write(proto: TProtocol, data: Any, index: Option[Int]): Unit = {
      proto.writeI16(data.asInstanceOf[Short])
    }
  }

  /**
   * Convert a thrift integer to a Spark SQL integer
   */
  class ThriftIntegerType extends ThriftDataType(IntegerType) {
    override def read(proto: TProtocol): Any = proto.readI32

    override def write(proto: TProtocol, data: Any, index: Option[Int]): Unit = {
      proto.writeI32(data.asInstanceOf[Int])
    }
  }

  /**
   * Convert a thrift enum to a Spark SQL datatype
   */
  class ThriftEnumType extends
    ThriftDataType(StructType(Array(StructField("value", IntegerType)))) {
    override def read(proto: TProtocol): Any = new GenericInternalRow(Array[Any](proto.readI32))

    override def write(proto: TProtocol, data: Any, index: Option[Int]): Unit = {
      val i32 = data match {
        case d: ArrayData => d.getInt(0)
        case _ => throw new IllegalArgumentException(
          s"Expecting Array of int but got something else.")
      }

      proto.writeI32(i32)
    }
  }

  /**
   * Convert a thrift enum to a Spark SQL datatype as string
   */
  class ThriftEnumAsStringType(val enumToString: Map[Int, UTF8String]) extends
    ThriftDataType(StringType) {
    override def read(proto: TProtocol): Any = enumToString.getOrElse(proto.readI32, null)

    override def write(proto: TProtocol, data: Any, index: Option[Int]): Unit = {
      proto.writeI32(enumToString.find(_._2.equals(data.asInstanceOf[UTF8String])).get._1)
    }
  }

  /**
   * Convert a thrift long to a Spark SQL long
   */
  class ThriftLongType extends ThriftDataType(LongType) {
    override def read(proto: TProtocol): Any = proto.readI64

    override def write(proto: TProtocol, data: Any, index: Option[Int]): Unit = {
      proto.writeI64(data.asInstanceOf[Long])
    }
  }

  /**
   * Convert a thrift double to a Spark SQL double
   */
  class ThriftDoubleType extends ThriftDataType(DoubleType) {
    override def read(proto: TProtocol): Any = proto.readDouble

    override def write(proto: TProtocol, data: Any, index: Option[Int]): Unit = {
      proto.writeDouble(data.asInstanceOf[Double])
    }
  }

  /**
   * Convert a thrift struct to a Spark SQL struct
   *
   * @param fields the struct fields of the
   */
  class ThriftStructType(val tStruct: TStruct, val fields: Seq[ThriftStructField], val nullIfNotSet: Boolean)
    extends ThriftDataType(StructType(fields.map(_.structField))) {

    // settableFieldsCount is used to keep count of non tunion fields, as
    // tunion fields are skipped over
    val (fieldIndex, settableFieldsCount) = {
      var index = 0
      (fields.map(field => {
        val key = field.id.toInt
        val fieldIndex = index
        // increment index only if the current field is not of TUnion type
        if (!field.thriftDataType.dataType.typeName.equals(NullType.typeName)) {
          index += 1
        }
        key -> (field, fieldIndex)
      }).toMap, index)
    }

    val indexesWithDefault: Iterable[(Int, Any)] = fieldIndex.values
      .collect { case (field, index) if field.defaultValue != null => (index, field.defaultValue) }

    protected def processStructType(proto: TProtocol, buffer: Array[Any],
                                    offset: Int = 0): Unit = {
      proto.readStructBegin()
      var currentFieldDef = proto.readFieldBegin()
      while (currentFieldDef.`type` != TType.STOP) {
        if (fieldIndex.contains(currentFieldDef.id)) {
          val (field, index) = fieldIndex(currentFieldDef.id)
          if (field.deserializeField &&
            (field.thriftType == currentFieldDef.`type` ||
              (field.thriftType == TType.I32 && currentFieldDef.`type` == TType.ENUM) ||
              (field.thriftType == TType.ENUM && currentFieldDef.`type` == TType.I32))) {
            buffer(offset + index) = field.thriftDataType.read(proto)
          } else {
            skip(proto, currentFieldDef.`type`)
          }
        } else {
          skip(proto, currentFieldDef.`type`)
        }
        proto.readFieldEnd()
        currentFieldDef = proto.readFieldBegin()
      }
      proto.readStructEnd()
    }

    override def read(proto: TProtocol): InternalRow = {
      val holder = new Array[Any](settableFieldsCount)
      if (!nullIfNotSet) {
        indexesWithDefault.foreach { case (index, default) => holder(index) = default }
      }
      processStructType(proto, holder)
      new GenericInternalRow(holder): InternalRow
    }

    /**
     * Skips fields without reading them.
     * The function is pulled from {@link TProtocolUtil.skip( TProtocol, byte)} and replaces
     * read field functions with consumeBuffer.
     */
    private def skip(proto: TProtocol, t: Byte): Unit = {
      val shortBytesSize = 2
      val intBytesSize = 4
      val longBytesSize = 8
      val doubleBytesSize = 8

      t match {
        case TType.BOOL =>
          proto.readBool()
        case TType.BYTE =>
          proto.readByte()
        case TType.I16 => {
          if (proto.isInstanceOf[TCompactProtocol]) {
            proto.readI16()
          } else {
            proto.getTransport.consumeBuffer(shortBytesSize)
          }
        }
        case TType.I32 => {
          if (proto.isInstanceOf[TCompactProtocol]) {
            proto.readI32()
          } else {
            proto.getTransport.consumeBuffer(intBytesSize)
          }
        }
        case TType.I64 => {
          if (proto.isInstanceOf[TCompactProtocol]) {
            proto.readI64()
          } else {
            proto.getTransport.consumeBuffer(longBytesSize)
          }
        }
        case TType.DOUBLE => {
          if (proto.isInstanceOf[TCompactProtocol]) {
            proto.readDouble()
          } else {
            proto.getTransport.consumeBuffer(doubleBytesSize)
          }
        }
        case TType.STRING => {
          val strLength = if (proto.isInstanceOf[TCompactProtocol]) {
            intToZigZag(proto.readI32)
          } else {
            proto.readI32
          }
          proto.getTransport.consumeBuffer(strLength)
        }
        case TType.ENUM => {
          proto.getTransport.consumeBuffer(intBytesSize)
        }
        case TType.STRUCT => {
          proto.readStructBegin()
          while (true) {
            val fieldDef = proto.readFieldBegin()
            if (fieldDef.`type` == 0) {
              proto.readStructEnd();
              return
            }
            skip(proto, fieldDef.`type`)
            proto.readFieldEnd()
          }
        }
        case TType.MAP => {
          val mapDef = proto.readMapBegin();

          (0 until mapDef.size).foreach(_ => {
            skip(proto, mapDef.keyType)
            skip(proto, mapDef.valueType)
          })
          proto.readMapEnd()
        }
        case TType.SET => {
          val setDef = proto.readSetBegin()
          (0 until setDef.size).foreach(_ => {
            skip(proto, setDef.elemType)
          })
          proto.readSetEnd()
        }
        case TType.LIST => {
          val listDef = proto.readListBegin()
          (0 until listDef.size).foreach(_ => {
            skip(proto, listDef.elemType)
          })
          proto.readListEnd()
        }
        case _ => {
          // throw new TException(s"Could not handle $t while skipping")
          TProtocolUtil.skip(proto, t)
        }
      }
    }

    override def write(proto: TProtocol, data: Any, index: Option[Int]): Unit = {
      val iRow = data match {
        case d: InternalRow =>
          index match {
            case Some(value) => d.getStruct(value, fields.length)
            case _ => d
          }
        case d: ArrayData =>
          index match {
            case Some(value) => d.getStruct(value, fields.length)
            case _ => throw new IllegalArgumentException(
              s"Expecting Struct but got Array for ${tStruct.name}.")
          }
      }

      proto.writeStructBegin(tStruct)
      var idx = 0
      fields.foreach(field => {
        field.tField.`type` match {
          case TType.ENUM => proto.writeFieldBegin(new TField(field.tField.name, TType.I32, field.tField.id))
          case _ => proto.writeFieldBegin(field.tField)
        }
        field.thriftDataType.write(proto, iRow.get(idx, field.thriftDataType.dataType), None)
        proto.writeFieldEnd()
        idx += 1
      })
      proto.writeFieldStop()
      proto.writeStructEnd()
    }
  }

  /**
   * Thrift Struct field that holds all information needed to deserialize the field
   *
   * @param id               thrift field id
   * @param thriftType       thrift type
   * @param name             field name
   * @param thriftDataType   converter from thrift type to Spark SQL's data type
   * @param defaultValue     default value if the field is absent
   * @param deserializeField if the field needs to be deserialized or not
   */
  class ThriftStructField(val id: Short, val thriftType: Short, val name: String,
                          val thriftDataType: ThriftDataType,
                          val defaultValue: Any,
                          val deserializeField: Boolean) extends Serializable {
    val structField: StructField = StructField(name, thriftDataType.dataType, nullable = true)
    val tField: TField = new TField(name, thriftType.byteValue(), id)
  }

  /**
   * Convert a thrift list to a Spark SQL array
   *
   * @param elementType the converter for list's elements
   */
  class ThriftListType(val elementTType: Short, val elementType: ThriftDataType) extends ThriftDataType(
    ArrayType(elementType.dataType)) {
    override def read(proto: TProtocol): Any = {
      val thriftDef = proto.readListBegin()
      val values = new Array[Any](thriftDef.size)
      (0 until thriftDef.size).foreach(i => values(i) = elementType.read(proto))
      proto.readListEnd()
      new GenericArrayData(values)
    }

    override def write(proto: TProtocol, data: Any, index: Option[Int]): Unit = {
      val iRow = data match {
        case d: InternalRow =>
          index match {
            case Some(value) => d.getArray(value)
            case _ => throw new IllegalArgumentException(
              s"Expecting Array but got Struct.")
          }
        case d: ArrayData =>
          index match {
            case Some(value) => d.getArray(value)
            case _ => d
          }
      }

      proto.writeListBegin(new TList(elementTType.toByte, iRow.numElements()))
      iRow.array.foreach(element => {
        elementType.write(proto, element, None)
      })
      proto.writeListEnd()
    }
  }

  /**
   * Convert a thrift set to a Spark SQL array
   *
   * @param elementType the converter for set's elements
   */
  class ThriftSetType(val elementTType: Short, val elementType: ThriftDataType) extends ThriftDataType(
    ArrayType(elementType.dataType)) {
    override def read(proto: TProtocol): Any = {
      val thriftDef = proto.readSetBegin()
      val values = new Array[Any](thriftDef.size)
      (0 until thriftDef.size).foreach(
        i => values(i) = elementType.read(proto))
      proto.readSetEnd()
      new GenericArrayData(values)
    }

    override def write(proto: TProtocol, data: Any, index: Option[Int]): Unit = {
      val iRow = data match {
        case d: InternalRow =>
          index match {
            case Some(value) => d.getArray(value)
            case _ => throw new IllegalArgumentException(
              s"Expecting Array but got Struct.")
          }
        case d: ArrayData =>
          index match {
            case Some(value) => d.getArray(value)
            case _ => d
          }
      }

      proto.writeSetBegin(new TSet(elementTType.toByte, iRow.numElements()))
      iRow.array.foreach(element => {
        elementType.write(proto, element, None)
      })
      proto.writeSetEnd()
    }
  }

  /**
   * Convert a thrift map to a Spark SQL map
   *
   * @param keyType   the converter for map's key
   * @param valueType the converter for map's value
   */
  class ThriftMapType(val keyTType: Short, val keyType: ThriftDataType,
                      val valTType: Short, val valueType: ThriftDataType)
    extends ThriftDataType(MapType(keyType.dataType, valueType.dataType)) {
    override def read(proto: TProtocol): Any = {
      val thriftDef = proto.readMapBegin()
      val keys = new Array[Any](thriftDef.size)
      val values = new Array[Any](thriftDef.size)
      (0 until thriftDef.size).foreach {
        i =>
          keys(i) = keyType.read(proto)
          values(i) = valueType.read(proto)
      }
      proto.readMapEnd()
      new ArrayBasedMapData(new GenericArrayData(keys), new GenericArrayData(values))
    }

    override def write(proto: TProtocol, data: Any, index: Option[Int]): Unit = {
      val iRow = data match {
        case d: InternalRow =>
          index match {
            case Some(value) => d.getMap(value)
            case _ => throw new IllegalArgumentException(
              s"Expecting Map but got Struct.")
          }
        case d: ArrayData =>
          index match {
            case Some(value) => d.getMap(value)
            case _ => throw new IllegalArgumentException(
              s"Expecting Map but got Array.")
          }
      }

      proto.writeMapBegin(new TMap(keyTType.toByte, valTType.toByte, iRow.numElements()))
      iRow.foreach(keyType.dataType, valueType.dataType, (key, value) => {
        keyType.write(proto, key, None)
        valueType.write(proto, value, None)
      })
      proto.writeMapEnd()
    }
  }

  /**
   * Convert a thrift union to a dummy type which is used to ignore thrift union fields
   */
  class ThriftNullType(val ttype: Byte) extends ThriftDataType(NullType) {
    override def read(proto: TProtocol): Any = {
      TProtocolUtil.skip(proto, ttype)
      None
    }

    override def write(proto: TProtocol, data: Any, index: Option[Int]): Unit = {
      // do nothing
    }
  }
}
