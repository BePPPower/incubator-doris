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

package org.apache.doris.datasource.trino.connector;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.trino.connector.TrinoConnectorManagers.TrinoConnectorConnectorManager.TrinoConnectorCatalogClassLoaderSupplier;
import org.apache.doris.datasource.trino.connector.TrinoConnectorManagers.TrinoConnectorConnectorManager.TrinoConnectorInternalConnectorFactory;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InitCatalogLog.Type;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.property.constants.TrinoConnectorProperties;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
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
import io.trino.metadata.InMemoryNodeManager;
import io.trino.metadata.MetadataManager;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.metadata.SessionPropertyManager;
import io.trino.metadata.TableHandle;
import io.trino.operator.GroupByHashPageIndexerFactory;
import io.trino.operator.PagesIndex;
import io.trino.operator.PagesIndexPageSorter;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.JoinCompiler;
import io.trino.testing.TestingSession;
import io.trino.type.BlockTypeOperators;
import io.trino.type.InternalTypeManager;
import io.trino.version.EmbedVersion;
import static java.util.Objects.requireNonNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class TrinoConnectorExternalCatalog extends ExternalCatalog {
    private static final Logger LOG = LogManager.getLogger(TrinoConnectorExternalCatalog.class);
    public static final String TRINO_CONNECTOR_FILESYSTEM = "filesystem";
    public static final String TRINO_CONNECTOR_HMS = "hms";

    private final List<String> TRINO_REQUIRED_PROPERTIES = ImmutableList.of(
            TrinoConnectorProperties.TRINO_CONNECTOR_NAME,
            TrinoConnectorProperties.TRINO_CONNECTOR_HIVE_METASTORE_URI,
            TrinoConnectorProperties.TRINO_CONNECTOR_HIVE_CONFIG_RESOURCES
    );

    private String catalogType = TRINO_CONNECTOR_HMS;
    private CatalogName trinoCatalogName;
    private Session trinoSession;
    private Connector connector;
    private SessionPropertyManager sessionPropertyManager;
    private final QueryIdGenerator queryIdGenerator = new QueryIdGenerator();

    public TrinoConnectorExternalCatalog(long catalogId, String name, String resource,
            Map<String, String> props, String comment) {
        super(catalogId, name, Type.TRINO_CONNECTOR, comment);
        Objects.requireNonNull(name, "catalogName is null");
        catalogProperty = new CatalogProperty(resource, props);
        trinoCatalogName = new CatalogName(name);
    }

    private void initConnector() {
        Map<String, String> trinoConnectorProperties = getTrinoConnectorProperties();
        String connectorName = trinoConnectorProperties.remove("connector.name");
        Objects.requireNonNull(connectorName, "connectorName is null");

        TrinoConnectorInternalConnectorFactory connectorFactory = Env.getCurrentEnv().getTrinoConnectorPluginManager()
                .getConnectorFactories().get(connectorName);
        Preconditions.checkArgument(connectorFactory != null, "No factory for connector '%s'.  Available factories: %s",
                connectorName,
                Env.getCurrentEnv().getTrinoConnectorPluginManager().getConnectorFactories().keySet());

        // create connector
        TrinoConnectorCatalogClassLoaderSupplier  duplicatePluginClassLoaderFactory= new TrinoConnectorCatalogClassLoaderSupplier(trinoCatalogName,
                connectorFactory.getDuplicatePluginClassLoaderFactory(), Env.getCurrentEnv().getTrinoConnectorPluginManager()
                .getHandleResolver());
        this.connector = createConnector(trinoCatalogName, connectorFactory.getConnectorFactory(),
                duplicatePluginClassLoaderFactory, trinoConnectorProperties);

        // return localQueryRunner.createCatalog2(name, connectorName, ImmutableMap.copyOf(trinoConnectorOptionsMap));
    }

    private void initTrinoSessionPropertyManager() {
        // create trino session
        Set<SystemSessionPropertiesProvider> extraSessionProperties = ImmutableSet.of();
        TaskManagerConfig taskManagerConfig = new TaskManagerConfig().setTaskConcurrency(4);
        FeaturesConfig featuresConfig = new FeaturesConfig();
        SessionPropertyManager sessionPropertyManager = createSessionPropertyManager(extraSessionProperties, taskManagerConfig, featuresConfig);
        sessionPropertyManager.addConnectorSessionProperties(trinoCatalogName, connector.getSessionProperties());
        this.sessionPropertyManager = sessionPropertyManager;
    }

    @Override
    protected void init() {
        super.init();
    }

    public String getCatalogType() {
        makeSureInitialized();
        return catalogType;
    }

    protected List<String> listDatabaseNames() {
        ConnectorSession connectorSession = trinoSession.toConnectorSession(trinoCatalogName);
        ConnectorTransactionHandle connectorTransactionHandle = this.connector.beginTransaction(
                IsolationLevel.READ_UNCOMMITTED, true, true);
        ConnectorMetadata connectorMetadata = this.connector.getMetadata(connectorSession, connectorTransactionHandle);
        return connectorMetadata.listSchemaNames(connectorSession);

        // return localQueryRunner.listSchemaNames(trinoSession, catalogName.getCatalogName());
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        makeSureInitialized();
        if (!trinoSession.getCatalog().isEmpty() && !trinoSession.getSchema().isEmpty()) {
            return getTrinoConnectorTable(dbName, tblName).isPresent();
        } else {
            return false;
        }

        // return localQueryRunner.tableExists(trinoSession, tblName);
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String dbName) {
        makeSureInitialized();
        QualifiedTablePrefix qualifiedTablePrefix = new QualifiedTablePrefix(trinoCatalogName.getCatalogName(), dbName);
        List<QualifiedObjectName> tables = trinoListTables(qualifiedTablePrefix);
        return tables.stream().map(field -> field.getObjectName()).collect(Collectors.toList());

        // List<QualifiedObjectName> tables = localQueryRunner.listTables(trinoSession, catalogName.getCatalogName(), dbName);
        // return tables.stream().map(field -> field.getObjectName()).collect(Collectors.toList());
    }

    private List<QualifiedObjectName> trinoListTables(QualifiedTablePrefix prefix) {
        Objects.requireNonNull(prefix, "prefix can not be null");

        Set<QualifiedObjectName> tables = new LinkedHashSet();
        ConnectorSession connectorSession = trinoSession.toConnectorSession(trinoCatalogName);
        ConnectorTransactionHandle connectorTransactionHandle = this.connector.beginTransaction(
                IsolationLevel.READ_UNCOMMITTED, true, true);
        ConnectorMetadata connectorMetadata = this.connector.getMetadata(connectorSession, connectorTransactionHandle);
        List<SchemaTableName> schemaTableNames = connectorMetadata.listTables(connectorSession, prefix.getSchemaName());
        List<QualifiedObjectName> tmpTables = new ArrayList<>();
        for (SchemaTableName schemaTableName : schemaTableNames) {
            QualifiedObjectName objName = QualifiedObjectName.convertFromSchemaTableName(prefix.getCatalogName())
                    .apply(schemaTableName);
            tmpTables.add(objName);
        }
        Objects.requireNonNull(tables);
        tmpTables.stream().filter(prefix::matches).forEach(tables::add);
        return ImmutableList.copyOf(tables);
    }

    public Optional<TableHandle> getTrinoConnectorTable(String dbName, String tblName) {
        makeSureInitialized();
        QualifiedObjectName tableName = new QualifiedObjectName(trinoCatalogName.getCatalogName(), dbName, tblName);

        if (!tableName.getCatalogName().isEmpty() &&
                !tableName.getSchemaName().isEmpty() &&
                !tableName.getObjectName().isEmpty()) {
            ConnectorSession connectorSession = trinoSession.toConnectorSession(trinoCatalogName);
            ConnectorTransactionHandle connectorTransactionHandle = this.connector.beginTransaction(
                    IsolationLevel.READ_UNCOMMITTED, true, true);
            return Optional.ofNullable(
                    this.connector.getMetadata(connectorSession, connectorTransactionHandle)
                            .getTableHandle(connectorSession, tableName.asSchemaTableName()))
                    .map((connectorTableHandle) -> {
                return new TableHandle(trinoCatalogName, connectorTableHandle, connectorTransactionHandle);
            });
        }
        return Optional.empty();

        // return localQueryRunner.getTableHandle(trinoSession, tableName);
    }



    private Connector createConnector(CatalogName catalogName, ConnectorFactory connectorFactory,
            Supplier<ClassLoader> duplicatePluginClassLoaderFactory, Map<String, String> properties) {
        // ConnectorContext context = new TrinoConenctorTestingConnectorContext(duplicatePluginClassLoaderFactory);

        InMemoryNodeManager inMemoryNodeManager = new InMemoryNodeManager();
        inMemoryNodeManager.addCurrentNodeConnector(catalogName);
        TypeManager typeManager = new InternalTypeManager(Env.getCurrentEnv().getTypeRegistry());
        TypeOperators typeOperators = Env.getCurrentEnv().getTypeRegistry().getTypeOperators();
        ConnectorContext context = new ConnectorContextInstance(
                new ConnectorAwareNodeManager(inMemoryNodeManager, "testenv", catalogName, true),
                EmbedVersion.testingVersionEmbedder(),
                typeManager,
                new InternalMetadataProvider(MetadataManager.createTestMetadataManager(Env.getCurrentEnv()
                        .getFeaturesConfig()), typeManager),
                new PagesIndexPageSorter(new PagesIndex.TestingFactory(false)),
                new GroupByHashPageIndexerFactory(new JoinCompiler(typeOperators), new BlockTypeOperators(typeOperators)),
                duplicatePluginClassLoaderFactory);



        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(connectorFactory.getClass().getClassLoader())) {
            return connectorFactory.create(catalogName.getCatalogName(), properties, context);
        }
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

    public Map<String, String> getTrinoConnectorProperties() {
        Map<String, String> properties = catalogProperty.getHadoopProperties();
        Map<String, String> trinoConnectorProperties = Maps.newHashMap();
        setTrinoConnectorExtraOptions(properties, trinoConnectorProperties);
        return trinoConnectorProperties;
    }

    private void setTrinoConnectorExtraOptions(Map<String, String> properties, Map<String, String> trinoConnectorProperties) {
        for (String trinoPropertyKey : TRINO_REQUIRED_PROPERTIES) {
            trinoConnectorProperties.put(trinoPropertyKey, properties.get(trinoPropertyKey));
        }
    }

    public Connector getConnector() {
        return connector;
    }

    public CatalogName getTrinoCatalogName() {
        return trinoCatalogName;
    }

    @Override
    protected void initLocalObjectsImpl() {
        initConnector();
        initTrinoSessionPropertyManager();
        this.trinoSession = TestingSession.testSessionBuilder(sessionPropertyManager).setQueryId(queryIdGenerator.createNextQueryId()).build();
    }

    @Override
    public void checkProperties() throws DdlException {
        super.checkProperties();
        for (String requiredProperty : TRINO_REQUIRED_PROPERTIES) {
            if (!catalogProperty.getProperties().containsKey(requiredProperty)) {
                throw new DdlException("Required property '" + requiredProperty + "' is missing");
            }
        }
    }

    public Session getTrinoSession() {
        return trinoSession;
    }
}
