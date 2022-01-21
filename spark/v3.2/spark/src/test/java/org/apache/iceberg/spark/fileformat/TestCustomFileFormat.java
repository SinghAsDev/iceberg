/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.spark.fileformat;

import com.twitter.data.proto.tutorial.thrift.AddressBook;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.hive.serde2.thrift.test.MyEnum;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.apache.iceberg.spark.fileformat.example.MiniStruct;
import org.apache.iceberg.spark.fileformat.example.schema.SparkNativeThriftSerDe;
import org.apache.iceberg.spark.fileformat.example.schema.ThriftSerDeOptions;
import org.apache.iceberg.spark.fileformat.example.schema.ThriftTypeToType;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runners.Parameterized;
import scala.Tuple2;

public class TestCustomFileFormat extends SparkCatalogTestBase {

  private static final String NAMESPACE = "default";

  @Parameterized.Parameters(name = "Catalog Name {0} - Options {2}")
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] {"spark_catalog", "org.apache.iceberg.spark.fileformat.example.CustomSparkCatalog",
                      ImmutableMap.builder()
                          .put("type", "hive")
                          .put("default-namespace", "default")
                          .put("parquet-enabled", "true")
                          .put(
                              "cache-enabled",
                              "false") // Spark will delete tables using v1, leaving the cache out of sync
                          .put(
                              "file-format-factory-impl",
                              "org.apache.iceberg.spark.fileformat.example.ExampleFileFormatFactory")
                          .build()
        },
        };
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private final String type;
  private final TableCatalog catalog;

  public TestCustomFileFormat(
      String catalogName,
      String implementation,
      Map<String, String> config) {
    super(catalogName, implementation, config);
    this.catalog = (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
    this.type = config.get("type");
  }

  @Before
  public void before() {
    spark.conf().set("hive.exec.dynamic.partition", "true");
    spark.conf().set("hive.exec.dynamic.partition.mode", "nonstrict");
    spark.conf().set("spark.sql.parquet.writeLegacyFormat", false);
  }

  @After
  public void after() throws IOException {
  }

  @Test
  public void testThriftTypeToType() {
    Type expectedSchema = Types.StructType.of(
        Types.NestedField.optional(1, "my_string", Types.StringType.get()),
        Types.NestedField.optional(2, "my_enum", Types.StringType.get())
        );
    Type actualSchema = new ThriftTypeToType().convert(MiniStruct.class);

    Assert.assertEquals(expectedSchema.asStructType(), actualSchema.asStructType());
  }

  @Test
  public void testThriftTypeToTypeComplex() {
    Type expectedSchema = Types.StructType.of(
        Types.NestedField.optional(11, "persons", Types.ListType.ofOptional(10,
            Types.StructType.of(
                Types.NestedField.required(6, "name", Types.StructType.of(
                    Types.NestedField.optional(1, "first_name", Types.StringType.get()),
                    Types.NestedField.optional(2, "last_name", Types.StringType.get())
                )),
                Types.NestedField.optional(7, "id", Types.IntegerType.get()),
                Types.NestedField.optional(8, "email", Types.StringType.get()),
                Types.NestedField.optional(9, "phones", Types.ListType.ofOptional(5,
                    Types.StructType.of(
                        Types.NestedField.optional(3, "number", Types.StringType.get()),
                        Types.NestedField.optional(4, "type", Types.StringType.get())
                    )
                ))
            )
        ))
    );

    Type actualSchema = new ThriftTypeToType().convert(AddressBook.class);

    Assert.assertEquals(expectedSchema.asStructType(), actualSchema.asStructType());
  }

  @Test
  public void testReadWrite() throws IOException, TException {
    String tableName = sourceName("testReadWrite");
    File location = temp.newFolder();

    sql("CREATE TABLE %s" +
        " USING ICEBERG" +
        " LOCATION '%s'" +
        " TBLPROPERTIES ('write.format.default' = 'thrift_sequencefile'," +
        "                'thrift_type' = '%s')", tableName, location, MiniStruct.class.getName());

    String testString = "test string one";
    MyEnum testEnum = MyEnum.LLAMA;
    sql("INSERT INTO %s VALUES ('" + testString + "', '" + testEnum.name() + "')", tableName);

    JavaRDD<Tuple2<NullWritable, BytesWritable>> keyValue =
        spark.sparkContext().sequenceFile(location.getPath() + "/data", NullWritable.class, BytesWritable.class).toJavaRDD();
    byte[] actualBytes = keyValue.map(Tuple2::_2).map(BytesWritable::getBytes).collect().get(0);

    MiniStruct miniStruct = new MiniStruct();
    miniStruct.setMy_string(testString);
    miniStruct.setMy_enum(testEnum);

    // byte[] expectedBytes = new TSerializer().serialize(miniStruct);

    TDeserializer tDeserializer = new TDeserializer();
    MiniStruct reconstructedMiniStruct = new MiniStruct();
    tDeserializer.deserialize(reconstructedMiniStruct, actualBytes);

    Assert.assertEquals(testString, reconstructedMiniStruct.getMy_string());
    Assert.assertEquals(testEnum, reconstructedMiniStruct.getMy_enum());

    List<Object[]> expected = sql(String.format("SELECT * FROM %s", tableName));
  }

  // @Test
  // public void testCustomFileFormat() throws Exception {
  //   Assume.assumeTrue("Can only migrate from Spark Session Catalog", catalog.name().equals("spark_catalog"));
  //
  //   MiniStruct miniStruct1 = new MiniStruct();
  //   miniStruct1.setMy_string("test string one");
  //   miniStruct1.setMy_enum(MyEnum.LLAMA);
  //
  //   MiniStruct miniStruct2 = new MiniStruct();
  //   miniStruct2.setMy_string("test string two");
  //   miniStruct2.setMy_enum(MyEnum.ALPACA);
  //
  //   File location = temp.newFolder();
  //
  //   TSerializer tSerializer = new TSerializer();
  //   String seqFilePath = String.format("%s/%s", location.getPath(), "testCustomFileFormat");
  //   JavaSparkContext.fromSparkContext(spark.sparkContext()).parallelize(
  //       Arrays.asList(
  //           tSerializer.serialize(miniStruct1),
  //           tSerializer.serialize(miniStruct2)), 1)
  //       .saveAsObjectFile(seqFilePath);
  //
  //   RDD<Tuple2<NullWritable, BytesWritable>> tuple2RDD =
  //       spark.sparkContext().sequenceFile(seqFilePath, NullWritable.class, BytesWritable.class);
  //
  //   String tableName = sourceName("testCustomFileFormat");
  //
  //   sql("CREATE TABLE %s (col1 binary)" +
  //       " USING SEQUENCEFILE" +
  //       // " STORED AS parquet" +
  //       " LOCATION '%s'", tableName, seqFilePath);
  //
  //   List<Object[]> expected = sql(String.format("SELECT * FROM %s", tableName));
  //
  //   // migrate table
  //   SparkActions.get().migrateTable(tableName).execute();
  //
  //   // check migrated table is returning expected result
  //   List<Object[]> results = sql("SELECT * FROM %s", tableName);
  //   Assert.assertTrue(results.size() > 0);
  //   assertEquals("Output must match", expected, results);
  // }

  private String sourceName(String source) {
    return NAMESPACE + "." + catalog.name() + "_" + type + "_" + source;
  }
}
