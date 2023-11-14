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

import org.apache.doris.common.jni.JniScanner;
import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.ScanPredicate;
import org.apache.doris.common.jni.vec.TableSchema;

import com.fasterxml.jackson.core.type.TypeReference;
import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.Session;
import io.trino.metadata.Split;
import io.trino.metadata.TableHandle;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.type.Type;
import io.trino.split.PageSourceProvider;
import io.trino.testing.TestingSession;
import static it.unimi.dsi.fastutil.HashCommon.nextPowerOfTwo;
import static java.lang.Math.min;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class TrinoConnectorJniScanner extends JniScanner {
    private static volatile int physicalProcessorCount = -1;
    private static final Logger LOG = LoggerFactory.getLogger(TrinoConnectorJniScanner.class);
    private static final String TRINO_CONNECTOR_OPTION_PREFIX = "trino_connector_option_prefix.";
    private final Map<String, String> trinoConnectorOptionParams;

    private final String catalogName;
    private final String dbName;
    private final String tblName;
    private final String trinoConnectorSplit;

    private final String trinoConnectorTableHandle;

    private final String trinoConnectorColumnHandles;

    private final String trinoConnectorColumnMetadata;
    private final String trinoConnectorPredicate;
    private TableHandle table;
    // private RecordReader<InternalRow> reader;
    private final TrinoConnectorColumnValue columnValue = new TrinoConnectorColumnValue();
    private List<String> trinoConnectorAllFieldNames;

    private LocalQueryRunner localQueryRunner;
    private PageSourceProvider pageSourceProvider;
    private ConnectorPageSource source;
    private Split split;
    private Session session;
    private DynamicFilter dynamicFilter = DynamicFilter.EMPTY;

    private List<ColumnHandle> columns;

    private List<TrinoColumnMetadata> columnMetadataList;

    public TrinoConnectorJniScanner(int batchSize, Map<String, String> params) {
        System.out.println("params:" + params);
        trinoConnectorSplit = params.get("trino_connector_split");
        trinoConnectorTableHandle = params.get("trino_connector_table_handle");
        trinoConnectorColumnHandles = params.get("trino_connector_column_handles");
        trinoConnectorColumnMetadata = params.get("trino_connector_column_metadata");
        trinoConnectorPredicate = params.get("trino_connector_predicate");
        catalogName = params.get("catalog_name");
        dbName = params.get("db_name");
        tblName = params.get("table_name");
        super.batchSize = batchSize;
        super.fields = params.get("trino_connector_column_names").split(",");
        super.predicates = new ScanPredicate[0];
        trinoConnectorOptionParams = params.entrySet().stream()
                .filter(kv -> kv.getKey().startsWith(TRINO_CONNECTOR_OPTION_PREFIX))
                .collect(Collectors
                        .toMap(kv1 -> kv1.getKey().substring(TRINO_CONNECTOR_OPTION_PREFIX.length()), kv1 -> kv1.getValue()));
        System.out.println("TrinoConnectorJniScanner ctor finished");
    }

    @Override
    public void open() throws IOException {
        System.out.println("open in java side");
        initTable();
        // initReader();
        parseRequiredTypes();
        System.out.println("open finished in java side");
    }

    private void initReader() throws IOException {
        // ReadBuilder readBuilder = table.newReadBuilder();
        // readBuilder.withProjection(getProjected());
        // readBuilder.withFilter(getPredicates());
        // reader = readBuilder.newRead().createReader(getSplit());
    }

    private int[] getProjected() {
        return Arrays.stream(fields).mapToInt(trinoConnectorAllFieldNames::indexOf).toArray();
    }

    // private List<Predicate> getPredicates() {
    //     List<Predicate> predicates = TrinoConnectorScannerUtils.decodeStringToObject(trinoConnectorPredicate);
    //     LOG.info("predicates:{}", predicates);
    //     return predicates;
    // }


    private void parseRequiredTypes() {
        ColumnType[] columnTypes = new ColumnType[fields.length];
        for (int i = 0; i < fields.length; i++) {
            int index = trinoConnectorAllFieldNames.indexOf(fields[i]);
            if (index == -1) {
                throw new RuntimeException(String.format("Cannot find field %s in schema %s",
                        fields[i], trinoConnectorAllFieldNames));
            }
            Type type = columnMetadataList.get(index).getType();
            System.out.println("type:" + type);
            columnTypes[i] = ColumnType.parseType(fields[i], TrinoTypeToHiveTypeTranslator.fromTrinoTypeToHiveType(type));
            System.out.println("hive_type:" + TrinoTypeToHiveTypeTranslator.fromTrinoTypeToHiveType(type));
            System.out.println("columnTypes:" + columnTypes[i].getType());
        }
        super.types = columnTypes;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    protected int getNext() throws IOException {
        int rows = 0;
        split = TrinoConnectorScannerUtils.decodeStringToObject(trinoConnectorSplit, Split.class, localQueryRunner.getObjectMapperProvider());
        if (split == null) {
            return 0;
        }
        if (source == null) {
            source = pageSourceProvider.createPageSource(session, split, table, columns, dynamicFilter);
        }
        Page page;
        while ((page = source.getNextPage()) != null) {
            // System.out.println("page.getChannelCount()1: " + page.getChannelCount());
            if (page != null) {
                // assure the page is in memory before handing to another operator
                page = page.getLoadedPage();
            }
            // System.out.println("page.getChannelCount()2: " + page.getChannelCount());
            // System.out.println("page.getPositionCount(): " + page.getPositionCount());
            for (int i = 0; i < page.getChannelCount(); ++i) {
                Block block = page.getBlock(i);
                columnValue.setBlock(block);
                columnValue.setColumnType(types[i]);
                for (int j = 0; j < page.getPositionCount(); ++j) {
                    columnValue.setPosition(j);
                    appendData(i, columnValue);
                }
            }
            rows += page.getPositionCount();
        }
        // System.out.println("rows: " + rows);
        return rows;
    }

    @Override
    protected TableSchema parseTableSchema() throws UnsupportedOperationException {
        // do nothing
        return null;
    }

    private void initTable() {
        System.out.println("initTable");

        //         List<String> stringList = new ArrayList<>();
//         EventListenerConfig eventListenerConfig = new EventListenerConfig();
//         // SessionPropertyManager spm = new SessionPropertyManager();
//         QueryManagerConfig qmc = new QueryManagerConfig();
//         // TaskManagerConfig tmc = new TaskManagerConfig();
//         MemoryManagerConfig mmc = new MemoryManagerConfig();
//         FeaturesConfig fc = new FeaturesConfig();
//         NodeMemoryConfig nmc = new NodeMemoryConfig();
//         DynamicFilterConfig dfc = new DynamicFilterConfig();
//         NodeSchedulerConfig nsc = new NodeSchedulerConfig();
//         Runtime.getRuntime().availableProcessors();
//         int taskConcurrency = getAvailablePhysicalProcessorCount();
//         if (physicalProcessorCount != -1) {
//             return;
//         }

        // try {
        //     new SystemInfo().getHardware()
        //             .getProcessor()
        //             .getPhysicalProcessorCount();
        // } catch (Throwable t) {
        //     t.printStackTrace();
        // }
//         String osArch = StandardSystemProperty.OS_ARCH.value();
//         // logical core count (including container cpu quota if there is any)
//         int availableProcessorCount = Runtime.getRuntime().availableProcessors();
//         int totalPhysicalProcessorCount;
//         if ("amd64".equals(osArch) || "x86_64".equals(osArch)) {
//             // Oshi can recognize physical processor count (without hyper threading) for x86 platforms.
//             // However, it doesn't correctly recognize physical processor count for ARM platforms.
//             // totalPhysicalProcessorCount = new SystemInfo()
//             //         .getHardware()
//             //         .getProcessor()
//             //         .getPhysicalProcessorCount();
//         }
//         else {
//             // ARM platforms do not support hyper threading, therefore each logical processor is separate core
//             totalPhysicalProcessorCount = availableProcessorCount;
//         }

        // cap available processor count to container cpu quota (if there is any).
        // physicalProcessorCount = min(totalPhysicalProcessorCount, availableProcessorCount);
        //         SessionBuilder sessionBuilder = Session.builder(new SessionPropertyManager());
        this.localQueryRunner = LocalQueryRunner.builder(TestingSession.testSessionBuilder()
                .build()).build();
        // session = ConnectContext.get().getSessionContext();
        session = localQueryRunner.getDefaultSession();
        String connectorName = (String)trinoConnectorOptionParams.remove("connector.name");
        trinoConnectorOptionParams.remove("type");
        trinoConnectorOptionParams.remove("create_time");
        localQueryRunner.createCatalog2(catalogName, connectorName, ImmutableMap.copyOf(trinoConnectorOptionParams));
        pageSourceProvider = localQueryRunner.getPageSourceManager();

        // CatalogName catalogName = createCatalog();
        // table = localQueryRunner.getTableHandle(session, new QualifiedObjectName(catalogName.getCatalogName(), dbName, tblName)).get();
        // columns = new ArrayList<>(localQueryRunner.getColumnHandles(session, table).values());
        // // trinoConnectorAllFieldNames = TrinoConnectorScannerUtils.fieldNames(table.rowType());
        // System.out.println("trinoConnectorAllFieldNames:" + trinoConnectorAllFieldNames);

        table = TrinoConnectorScannerUtils.decodeStringToObject(trinoConnectorTableHandle, TableHandle.class, localQueryRunner.getObjectMapperProvider());
        io.airlift.json.JsonCodec<List<ColumnHandle>> columnHandleCodec = new JsonCodecFactory(localQueryRunner.getObjectMapperProvider())
                .listJsonCodec(ColumnHandle.class);
        columns = columnHandleCodec.fromJson(trinoConnectorColumnHandles);
        io.airlift.json.JsonCodec<List<TrinoColumnMetadata>> columnMetadataCodec = new JsonCodecFactory(localQueryRunner.getObjectMapperProvider())
                .listJsonCodec(TrinoColumnMetadata.class);
        columnMetadataList = columnMetadataCodec.fromJson(trinoConnectorColumnMetadata);
        System.out.println("table: " + table);
        System.out.println("columns: " + columns);
        System.out.println("columnMetadataList: " + columnMetadataList);
        System.out.println("trinoConnectorOptionParams: " + trinoConnectorOptionParams);
        System.out.println("initTable finished");
        trinoConnectorAllFieldNames = TrinoConnectorScannerUtils.fieldNames(columnMetadataList);
    }
}
