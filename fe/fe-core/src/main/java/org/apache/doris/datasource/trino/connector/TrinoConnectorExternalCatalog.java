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
import org.apache.doris.datasource.property.constants.HMSProperties;
import org.apache.doris.trino.connector.LocalQueryRunner;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InitCatalogLog.Type;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.property.constants.TrinoConnectorProperties;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.trino.Session;
import io.trino.connector.CatalogName;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class TrinoConnectorExternalCatalog extends ExternalCatalog {
    private static final Logger LOG = LogManager.getLogger(TrinoConnectorExternalCatalog.class);
    public static final String TRINO_CONNECTOR_CATALOG_TYPE = "trino.connector.catalog.type";
    public static final String TRINO_CONNECTOR_FILESYSTEM = "filesystem";
    public static final String TRINO_CONNECTOR_HMS = "hms";
    protected String catalogType;
    protected CatalogName catalogName;
    // protected ConnectorManager connectorManager;
    // protected MetadataManager metadataManager;
    protected Session session;

    protected LocalQueryRunner localQueryRunner;

    private static final List<String> REQUIRED_PROPERTIES = ImmutableList.of(
            TrinoConnectorProperties.TRINO_CONNECTOR_NAME
    );

    public TrinoConnectorExternalCatalog(long catalogId, String name, String resource,
            Map<String, String> props, String comment) {
        super(catalogId, name, Type.TRINO_CONNECTOR, comment);
        catalogProperty = new CatalogProperty(resource, props);
        localQueryRunner = Env.getCurrentEnv().getLocalQueryRunner();
        session = localQueryRunner.getDefaultSession();
    }

    @Override
    protected void init() {
        super.init();
    }

    public CatalogName getCatalog() {
        makeSureInitialized();
        return catalogName;
    }

    public String getCatalogType() {
        makeSureInitialized();
        return catalogType;
    }

    protected List<String> listDatabaseNames() {
        return localQueryRunner.listSchemaNames(session, catalogName.getCatalogName());
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        makeSureInitialized();
        return localQueryRunner.tableExists(session, tblName);
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String dbName) {
        makeSureInitialized();
        List<QualifiedObjectName> tables = localQueryRunner.listTables(session, catalogName.getCatalogName(), dbName);
        return tables.stream().map(field -> field.getObjectName()).collect(Collectors.toList());
    }

    public Optional<TableHandle> getTrinoConnectorTable(String dbName, String tblName) {
        makeSureInitialized();
        return localQueryRunner.getTableHandle(session, new QualifiedObjectName(catalogName.getCatalogName(), dbName, tblName));
    }

    protected String getTrinoConnectorCatalogType(String catalogType) {
        if (TRINO_CONNECTOR_HMS.equalsIgnoreCase(catalogType)) {
            return TrinoConnectorProperties.TRINO_CONNECTOR_HMS_CATALOG;
        } else {
            return TrinoConnectorProperties.TRINO_CONNECTOR_FILESYSTEM_CATALOG;
        }
    }

    protected CatalogName createCatalog() {
        Map<String, String> trinoConnectorOptionsMap = getTrinoConnectorOptionsMap();
        String connectorName = (String)trinoConnectorOptionsMap.remove("connector.name");
        trinoConnectorOptionsMap.remove("type");
        trinoConnectorOptionsMap.remove("create_time");
        return localQueryRunner.createCatalog2(name, connectorName, ImmutableMap.copyOf(trinoConnectorOptionsMap));
    }

    public Map<String, String> getTrinoConnectorOptionsMap() {
        Map<String, String> properties = catalogProperty.getHadoopProperties();
        Map<String, String> options = Maps.newHashMap();
        options.put(TrinoConnectorProperties.TRINO_CONNECTOR_NAME, properties.get(TrinoConnectorProperties.TRINO_CONNECTOR_NAME));
        setTrinoConnectorCatalogOptions(properties, options);
        setTrinoConnectorExtraOptions(properties, options);
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
}
