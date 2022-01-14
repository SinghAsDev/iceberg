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

package org.apache.iceberg;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.fileformat.CustomFileFormat;
import org.apache.iceberg.types.Comparators;

/**
 * Enum of supported file formats.
 */
@SuppressWarnings({"checkstyle:VisibilityModifier", "checkstyle:StaticVariableName"})
public class FileFormat implements Serializable {
  public static FileFormat ORC = new FileFormat("ORC", "orc", true, null);
  public static FileFormat PARQUET = new FileFormat("PARQUET", "parquet", true, null);
  public static FileFormat AVRO = new FileFormat("AVRO", "avro", true, null);
  public static FileFormat METADATA = new FileFormat("METADATA", "metadata.json", false, null);

  private static final Map<String, FileFormat> inbuiltFormats = Stream.of(
      ORC,
      PARQUET,
      AVRO,
      METADATA
  ).collect(Collectors.toMap(FileFormat::name, Function.identity()));

  private final String name;
  private final String ext;
  private final boolean splittable;
  private final CustomFileFormat customFileFormat;

  public FileFormat(
      String name,
      String ext,
      boolean splittable,
      CustomFileFormat customFileFormat) {
    this.name = name;
    this.ext = "." + ext;
    this.splittable = splittable;
    this.customFileFormat = customFileFormat;
  }

  public static FileFormat valueOf(String name) {
    return inbuiltFormats.get(name);
  }

  public boolean isSplittable() {
    return splittable;
  }

  /**
   * Returns filename with this format's extension added, if necessary.
   *
   * @param filename a filename or path
   * @return if the ext is present, the filename, otherwise the filename with ext added
   */
  public String addExtension(String filename) {
    if (filename.endsWith(ext)) {
      return filename;
    }
    return filename + ext;
  }

  public static FileFormat fromFileName(CharSequence filename) {
    for (FileFormat format : inbuiltFormats.values()) {
      int extStart = filename.length() - format.ext.length();
      if (Comparators.charSequences().compare(format.ext, filename.subSequence(extStart, filename.length())) == 0) {
        return format;
      }
    }

    return null;
  }

  public String name() {
    return name;
  }

  @Override
  public String toString() {
    return name;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return  true;
    }

    if (!(o instanceof FileFormat)) {
      return false;
    }

    return ((FileFormat) o).name().equals(this.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, ext, splittable);
  }

  public CustomFileFormat getCustomFileFormat() {
    return customFileFormat;
  }
}
