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
import org.apache.doris.datasource.trino.connector.TrinoConnectorManagers.TrinoConnectorConnectorManager.TrinoConenctorTestingConnectorContext;
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
import io.trino.execution.DynamicFilterConfig;
import io.trino.execution.QueryIdGenerator;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.TaskManagerConfig;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.memory.MemoryManagerConfig;
import io.trino.memory.NodeMemoryConfig;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.metadata.SessionPropertyManager;
import io.trino.metadata.TableHandle;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.testing.TestingSession;
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
    public static final String TRINO_CONNECTOR_CATALOG_TYPE = "trino.connector.catalog.type";
    public static final String TRINO_CONNECTOR_FILESYSTEM = "filesystem";
    public static final String TRINO_CONNECTOR_HMS = "hms";
    protected String catalogType;
    protected CatalogName catalogName;
    // protected MetadataManager metadataManager;
    protected Session trinoSession;

    private Connector connector;

    private final QueryIdGenerator queryIdGenerator = new QueryIdGenerator();

    private static final List<String> REQUIRED_PROPERTIES = ImmutableList.of(
            TrinoConnectorProperties.TRINO_CONNECTOR_NAME
    );

    public TrinoConnectorExternalCatalog(long catalogId, String name, String resource,
            Map<String, String> props, String comment) {
        super(catalogId, name, Type.TRINO_CONNECTOR, comment);
        catalogProperty = new CatalogProperty(resource, props);
    }

    public CatalogName getCatalog() {
        makeSureInitialized();
        return catalogName;
    }

    private CatalogName createCatalog() {
        Map<String, String> trinoConnectorOptionsMap = getTrinoConnectorOptionsMap();
        String connectorName = (String)trinoConnectorOptionsMap.remove("connector.name");
        Objects.requireNonNull(connectorName, "connectorName is null");

        TrinoConnectorInternalConnectorFactory connectorFactory = Env.getCurrentEnv().getTrinoConnectorPluginManager()
                .getConnectorFactories().get(connectorName);
        Preconditions.checkArgument(connectorFactory != null, "No factory for connector '%s'.  Available factories: %s",
                connectorName,
                Env.getCurrentEnv().getTrinoConnectorPluginManager().getConnectorFactories().keySet());

        Objects.requireNonNull(name, "catalogName is null");
        Objects.requireNonNull(trinoConnectorOptionsMap, "properties is null");
        Objects.requireNonNull(connectorFactory, "connectorFactory is null");

        // TODO(ftw): check if this catalogName is existed?
        CatalogName catalog = new CatalogName(name);

        // create connector
        TrinoConnectorCatalogClassLoaderSupplier  duplicatePluginClassLoaderFactory= new TrinoConnectorCatalogClassLoaderSupplier(catalog,
                connectorFactory.getDuplicatePluginClassLoaderFactory(), Env.getCurrentEnv().getTrinoConnectorPluginManager()
                .getHandleResolver());
        this.connector = createConnector(catalog, connectorFactory.getConnectorFactory(),
                duplicatePluginClassLoaderFactory, trinoConnectorOptionsMap);

        // create trino session
        Set<SystemSessionPropertiesProvider> extraSessionProperties = ImmutableSet.of();
        TaskManagerConfig taskManagerConfig = new TaskManagerConfig().setTaskConcurrency(4);
        FeaturesConfig featuresConfig = new FeaturesConfig();
        SessionPropertyManager sessionPropertyManager = createSessionPropertyManager(extraSessionProperties, taskManagerConfig, featuresConfig);
        sessionPropertyManager.addConnectorSessionProperties(catalog, connector.getSessionProperties());
        this.trinoSession = testSessionBuilder(sessionPropertyManager).setQueryId(queryIdGenerator.createNextQueryId()).build();

        return catalog;

        // return localQueryRunner.createCatalog2(name, connectorName, ImmutableMap.copyOf(trinoConnectorOptionsMap));
    }

    private Session.SessionBuilder testSessionBuilder(SessionPropertyManager sessionPropertyManager) {
        return TestingSession.testSessionBuilder(sessionPropertyManager);
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
        ConnectorSession connectorSession = trinoSession.toConnectorSession(catalogName);
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
        QualifiedTablePrefix qualifiedTablePrefix = new QualifiedTablePrefix(catalogName.getCatalogName(), dbName);
        List<QualifiedObjectName> tables = trinoListTables(qualifiedTablePrefix);
        return tables.stream().map(field -> field.getObjectName()).collect(Collectors.toList());

        // List<QualifiedObjectName> tables = localQueryRunner.listTables(trinoSession, catalogName.getCatalogName(), dbName);
        // return tables.stream().map(field -> field.getObjectName()).collect(Collectors.toList());
    }

    private List<QualifiedObjectName> trinoListTables(QualifiedTablePrefix prefix) {
        Objects.requireNonNull(prefix, "prefix can not be null");

        Set<QualifiedObjectName> tables = new LinkedHashSet();
        ConnectorSession connectorSession = trinoSession.toConnectorSession(catalogName);
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
        QualifiedObjectName tableName = new QualifiedObjectName(catalogName.getCatalogName(), dbName, tblName);

        if (!tableName.getCatalogName().isEmpty() &&
                !tableName.getSchemaName().isEmpty() &&
                !tableName.getObjectName().isEmpty()) {
            ConnectorSession connectorSession = trinoSession.toConnectorSession(catalogName);
            ConnectorTransactionHandle connectorTransactionHandle = this.connector.beginTransaction(
                    IsolationLevel.READ_UNCOMMITTED, true, true);
            return Optional.ofNullable(
                    this.connector.getMetadata(connectorSession, connectorTransactionHandle)
                            .getTableHandle(connectorSession, tableName.asSchemaTableName()))
                    .map((connectorTableHandle) -> {
                return new TableHandle(catalogName, connectorTableHandle, connectorTransactionHandle);
            });
        }
        return Optional.empty();

        // return localQueryRunner.getTableHandle(trinoSession, tableName);
    }



    private Connector createConnector(CatalogName catalogName, ConnectorFactory connectorFactory, Supplier<ClassLoader> duplicatePluginClassLoaderFactory, Map<String, String> properties) {
        ConnectorContext context = new TrinoConenctorTestingConnectorContext(duplicatePluginClassLoaderFactory);
        ThreadContextClassLoader ignored = new ThreadContextClassLoader(connectorFactory.getClass().getClassLoader());

        Connector var7;
        try {
            var7 = connectorFactory.create(catalogName.getCatalogName(), properties, context);
        } catch (Throwable var10) {
            try {
                ignored.close();
            } catch (Throwable var9) {
                var10.addSuppressed(var9);
            }

            throw var10;
        }

        ignored.close();
        return var7;
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

    public Map<String, String> getTrinoConnectorOptionsMap() {
        Map<String, String> properties = catalogProperty.getHadoopProperties();
        Map<String, String> options = Maps.newHashMap();
        options.put(TrinoConnectorProperties.TRINO_CONNECTOR_NAME, properties.get(TrinoConnectorProperties.TRINO_CONNECTOR_NAME));
        setTrinoConnectorCatalogOptions(properties, options);
        setTrinoConnectorExtraOptions(properties, options);
        options.remove("create_time");
        options.remove("type");
        return options;
    }

    private void setTrinoConnectorCatalogOptions(Map<String, String> properties, Map<String, String> options) {
        // options.put(TrinoConnectorProperties.TRINO_CONNECTOR_CATALOG_TYPE, getTrinoConnectorCatalogType(catalogType));
        // options.put(TrinoConnectorProperties.HIVE_METASTORE_URIS, properties.get(HMSProperties.HIVE_METASTORE_URIS));
    }

    private void setTrinoConnectorExtraOptions(Map<String, String> properties, Map<String, String> options) {
        for (Map.Entry<String, String> kv : properties.entrySet()) {
            if (kv.getKey().startsWith(TrinoConnectorProperties.TRINO_CONNECTOR_PREFIX)) {
                options.put(kv.getKey().substring(TrinoConnectorProperties.TRINO_CONNECTOR_PREFIX.length()), kv.getValue());
            }
        }
    }

    public Connector getConnector() {
        return connector;
    }

    public CatalogName getCatalogName() {
        return catalogName;
    }

    @Override
    protected void initLocalObjectsImpl() {
        catalogType = TRINO_CONNECTOR_HMS;
        catalogName = createCatalog();
    }

    @Override
    public void checkProperties() throws DdlException {
        super.checkProperties();
        for (String requiredProperty : REQUIRED_PROPERTIES) {
            if (!catalogProperty.getProperties().containsKey(requiredProperty)) {
                throw new DdlException("Required property '" + requiredProperty + "' is missing");
            }
        }
    }

    public Session getTrinoSession() {
        return trinoSession;
    }
}
