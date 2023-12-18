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

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.FeaturesConfig;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.SystemSessionPropertiesProvider;
import io.trino.connector.CatalogName;
import io.trino.connector.ConnectorAwareNodeManager;
import io.trino.connector.ConnectorContextInstance;
import io.trino.connector.InternalMetadataProvider;
import io.trino.execution.DynamicFilterConfig;
import io.trino.execution.QueryIdGenerator;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.TaskManagerConfig;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.memory.MemoryManagerConfig;
import io.trino.memory.NodeMemoryConfig;
import io.trino.metadata.HandleJsonModule;
import io.trino.metadata.HandleResolver;
import io.trino.metadata.InMemoryNodeManager;
import io.trino.metadata.MetadataManager;
import io.trino.metadata.SessionPropertyManager;
import io.trino.metadata.Split;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TypeRegistry;
import io.trino.operator.GroupByHashPageIndexerFactory;
import io.trino.operator.PagesIndex;
import io.trino.operator.PagesIndexPageSorter;
import io.trino.plugin.base.TypeDeserializer;
import io.trino.server.ServerPluginsProvider;
import io.trino.server.ServerPluginsProviderConfig;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.JoinCompiler;
import io.trino.testing.TestingSession;
import io.trino.type.BlockTypeOperators;
import io.trino.type.InternalTypeManager;
import io.trino.version.EmbedVersion;
import static java.util.Objects.requireNonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
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
    private final String trinoSessionString;
    private final String trinoTrascationHandleString;
    private TableHandle table;
    private final TrinoConnectorColumnValue columnValue = new TrinoConnectorColumnValue();
    private List<String> trinoConnectorAllFieldNames;

    private ConnectorPageSourceProvider pageSourceProvider;
    private ConnectorPageSource source;
    private Split split;
    private Session session;
    private DynamicFilter dynamicFilter = DynamicFilter.EMPTY;

    private List<ColumnHandle> columns;

    private List<TrinoColumnMetadata> columnMetadataList;

    private TrinoConnectorPluginManager trinoConnectorPluginManager;

    private ObjectMapperProvider objectMapperProvider;

    private Connector connector;

    private ConnectorTransactionHandle connectorTransactionHandle;
    private final QueryIdGenerator queryIdGenerator = new QueryIdGenerator();
    private CatalogName catalog;
    private FeaturesConfig featuresConfig;
    private TypeRegistry typeRegistry;

    public TrinoConnectorJniScanner(int batchSize, Map<String, String> params) {
        LOG.info("params:" + params);

        trinoConnectorSplit = params.get("trino_connector_split");
        trinoConnectorTableHandle = params.get("trino_connector_table_handle");
        trinoConnectorColumnHandles = params.get("trino_connector_column_handles");
        trinoConnectorColumnMetadata = params.get("trino_connector_column_metadata");
        trinoConnectorPredicate = params.get("trino_connector_predicate");
        trinoSessionString = params.get("trino_connector_session");
        trinoTrascationHandleString = params.get("trino_connector_trascation_handle");


        catalogName = params.get("catalog_name");
        catalog = new CatalogName(catalogName);
        dbName = params.get("db_name");
        tblName = params.get("table_name");

        super.batchSize = batchSize;
        super.fields = params.get("trino_connector_column_names").split(",");

        super.predicates = new ScanPredicate[0];
        trinoConnectorOptionParams = params.entrySet().stream()
                .filter(kv -> kv.getKey().startsWith(TRINO_CONNECTOR_OPTION_PREFIX))
                .collect(Collectors
                        .toMap(kv1 -> kv1.getKey().substring(TRINO_CONNECTOR_OPTION_PREFIX.length()), kv1 -> kv1.getValue()));
    }

    @Override
    public void open() throws IOException {
        LOG.info("open in java side");
        initTable();
        parseRequiredTypes();
        LOG.info("open finished in java side");
    }

    private void parseRequiredTypes() {
        ColumnType[] columnTypes = new ColumnType[fields.length];
        for (int i = 0; i < fields.length; i++) {
            int index = trinoConnectorAllFieldNames.indexOf(fields[i]);
            if (index == -1) {
                throw new RuntimeException(String.format("Cannot find field %s in schema %s",
                        fields[i], trinoConnectorAllFieldNames));
            }
            Type type = columnMetadataList.get(index).getType();
            LOG.info("type:" + type);
            columnTypes[i] = ColumnType.parseType(fields[i], TrinoTypeToHiveTypeTranslator.fromTrinoTypeToHiveType(type));
            LOG.info("hive_type:" + TrinoTypeToHiveTypeTranslator.fromTrinoTypeToHiveType(type));
            LOG.info("columnTypes:" + columnTypes[i].getType());
        }
        super.types = columnTypes;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    protected int getNext() throws IOException {
        int rows = 0;
        split = TrinoConnectorScannerUtils.decodeStringToObject(trinoConnectorSplit, Split.class, this.objectMapperProvider);
        if (split == null) {
            return 0;
        }
        if (source == null) {
            source = pageSourceProvider.createPageSource(connectorTransactionHandle, session.toConnectorSession(catalogName),
                    split.getConnectorSplit(), table.getConnectorHandle(), columns, dynamicFilter);
        }
        Page page;
        while ((page = source.getNextPage()) != null) {
            if (page != null) {
                // assure the page is in memory before handing to another operator
                page = page.getLoadedPage();
            }
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
        return rows;
    }

    @Override
    protected TableSchema parseTableSchema() throws UnsupportedOperationException {
        // do nothing
        return null;
    }

    private void initTable() {
        try {
            initSpiEnvironment();
            initConnector(this.catalog);

            // session = localQueryRunner.getDefaultSession();
            String connectorName = (String)trinoConnectorOptionParams.remove("connector.name");
            trinoConnectorOptionParams.remove("type");
            trinoConnectorOptionParams.remove("create_time");

            pageSourceProvider = connector.getPageSourceProvider();
            // pageSourceProvider = localQueryRunner.getPageSourceManager();

            // mock ObjectMapperProvider
            generateObjectMapperProvider();

            createSession(this.catalog);

            connectorTransactionHandle = TrinoConnectorScannerUtils.decodeStringToObject(trinoTrascationHandleString, ConnectorTransactionHandle.class, this.objectMapperProvider);

            table = TrinoConnectorScannerUtils.decodeStringToObject(trinoConnectorTableHandle, TableHandle.class, this.objectMapperProvider);
            io.airlift.json.JsonCodec<List<ColumnHandle>> columnHandleCodec = new JsonCodecFactory(this.objectMapperProvider)
                    .listJsonCodec(ColumnHandle.class);
            columns = columnHandleCodec.fromJson(trinoConnectorColumnHandles);
            io.airlift.json.JsonCodec<List<TrinoColumnMetadata>> columnMetadataCodec = new JsonCodecFactory(this.objectMapperProvider)
                    .listJsonCodec(TrinoColumnMetadata.class);
            columnMetadataList = columnMetadataCodec.fromJson(trinoConnectorColumnMetadata);
            trinoConnectorAllFieldNames = TrinoConnectorScannerUtils.fieldNames(columnMetadataList);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("get exception: " + e.getMessage());
            throw e;
        }
    }

    private void initSpiEnvironment() {
        TypeOperators typeOperators = new TypeOperators();
        this.featuresConfig = new FeaturesConfig();
        this.typeRegistry = new TypeRegistry(typeOperators, featuresConfig);

        ServerPluginsProvider serverPluginsProvider = new ServerPluginsProvider(new ServerPluginsProviderConfig(),
                directExecutor());
        HandleResolver handleResolver = new HandleResolver();
        trinoConnectorPluginManager = new TrinoConnectorPluginManager(serverPluginsProvider,
                typeRegistry, handleResolver);
        trinoConnectorPluginManager.loadPlugins();
    }

    private void generateObjectMapperProvider() {
        TypeManager typeManager = new InternalTypeManager(this.trinoConnectorPluginManager.getTypeRegistry());
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        Set<Module> modules = new HashSet<Module>();
        HandleResolver handleResolver = this.trinoConnectorPluginManager.getHandleResolver();
        modules.add(HandleJsonModule.tableHandleModule(handleResolver));
        modules.add(HandleJsonModule.columnHandleModule(handleResolver));
        modules.add(HandleJsonModule.splitModule(handleResolver));
        modules.add(HandleJsonModule.outputTableHandleModule(handleResolver));
        modules.add(HandleJsonModule.insertTableHandleModule(handleResolver));
        modules.add(HandleJsonModule.tableExecuteHandleModule(handleResolver));
        modules.add(HandleJsonModule.indexHandleModule(handleResolver));
        modules.add(HandleJsonModule.transactionHandleModule(handleResolver));
        modules.add(HandleJsonModule.partitioningHandleModule(handleResolver));
        objectMapperProvider.setModules(modules);
        objectMapperProvider.setJsonDeserializers(ImmutableMap.of(io.trino.spi.type.Type.class, new TypeDeserializer(typeManager)));
        this.objectMapperProvider = objectMapperProvider;
    }

    private void initConnector(CatalogName catalog) {
        String connectorName = trinoConnectorOptionParams.remove("connector.name");
        TrinoConnectorInternalConnectorFactory connectorFactory = this.trinoConnectorPluginManager
                .getConnectorFactories().get(connectorName);
        TrinoConnectorCatalogClassLoaderSupplier  duplicatePluginClassLoaderFactory= new TrinoConnectorCatalogClassLoaderSupplier(catalog,
                connectorFactory.getDuplicatePluginClassLoaderFactory(), this.trinoConnectorPluginManager.getHandleResolver());

        this.connector = createConnector(catalog, connectorFactory.getConnectorFactory(),
                duplicatePluginClassLoaderFactory, this.trinoConnectorOptionParams);
    }

    private Connector createConnector(
            CatalogName catalogName, ConnectorFactory connectorFactory,
            Supplier<ClassLoader> duplicatePluginClassLoaderFactory, Map<String, String> properties) {
        InMemoryNodeManager inMemoryNodeManager = new InMemoryNodeManager();
        inMemoryNodeManager.addCurrentNodeConnector(catalogName);
        TypeManager typeManager = new InternalTypeManager(this.typeRegistry);
        TypeOperators typeOperators = this.typeRegistry.getTypeOperators();
        ConnectorContext context = new ConnectorContextInstance(
                new ConnectorAwareNodeManager(inMemoryNodeManager, "testenv", catalogName, true),
                EmbedVersion.testingVersionEmbedder(),
                typeManager,
                new InternalMetadataProvider(MetadataManager.createTestMetadataManager(this.featuresConfig), typeManager),
                new PagesIndexPageSorter(new PagesIndex.TestingFactory(false)),
                new GroupByHashPageIndexerFactory(new JoinCompiler(typeOperators), new BlockTypeOperators(typeOperators)),
                duplicatePluginClassLoaderFactory);

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(connectorFactory.getClass().getClassLoader())) {
            return connectorFactory.create(catalogName.getCatalogName(), properties, context);
        }
    }

    private void createSession(CatalogName catalog) {
        // create trino session
        Set<SystemSessionPropertiesProvider> extraSessionProperties = ImmutableSet.of();
        TaskManagerConfig taskManagerConfig = new TaskManagerConfig().setTaskConcurrency(4);
        FeaturesConfig featuresConfig = new FeaturesConfig();
        SessionPropertyManager sessionPropertyManager = createSessionPropertyManager(extraSessionProperties, taskManagerConfig, featuresConfig);
        sessionPropertyManager.addConnectorSessionProperties(catalog, connector.getSessionProperties());
        this.session = testSessionBuilder(sessionPropertyManager).setQueryId(queryIdGenerator.createNextQueryId()).build();
    }

    private SessionPropertyManager createSessionPropertyManager(
            Set<SystemSessionPropertiesProvider> extraSessionProperties,
            TaskManagerConfig taskManagerConfig,
            FeaturesConfig featuresConfig)
    {
        Set<SystemSessionPropertiesProvider> systemSessionProperties = ImmutableSet.<SystemSessionPropertiesProvider>builder()
                .addAll(requireNonNull(extraSessionProperties, "extraSessionProperties is null"))
                .add(new SystemSessionProperties(
                        new QueryManagerConfig(),
                        taskManagerConfig,
                        new MemoryManagerConfig(),
                        featuresConfig,
                        new NodeMemoryConfig(),
                        new DynamicFilterConfig(),
                        new NodeSchedulerConfig()))
                .build();

        return new SessionPropertyManager(systemSessionProperties);
    }

    private Session.SessionBuilder testSessionBuilder(SessionPropertyManager sessionPropertyManager) {
        return TestingSession.testSessionBuilder(sessionPropertyManager);
    }
}
