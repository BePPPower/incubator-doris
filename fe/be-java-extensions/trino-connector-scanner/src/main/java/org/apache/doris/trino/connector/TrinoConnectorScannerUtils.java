// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.trino.connector;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.metadata.HandleResolver;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import static org.apache.doris.trino.connector.JsonCodec.jsonCodec;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

public class TrinoConnectorScannerUtils {
    private static final Base64.Decoder BASE64_DECODER = Base64.getUrlDecoder();

    public static <T> T decodeStringToObject(String encodedStr, Class<T> type, ObjectMapperProvider objectMapperProvider) {
        try {
            io.airlift.json.JsonCodec<T> jsonCodec = (io.airlift.json.JsonCodec<T>) new JsonCodecFactory(objectMapperProvider).jsonCodec(type);
            return jsonCodec.fromJson(encodedStr);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static List<String> fieldNames(List<TrinoColumnMetadata> columnMetadataList) {
        // return rowType.getFields().stream()
        //         .map(DataField::name)
        //         .collect(Collectors.toList());
        return columnMetadataList.stream().map(x->x.getName()).collect(Collectors.toList());
    }
}
