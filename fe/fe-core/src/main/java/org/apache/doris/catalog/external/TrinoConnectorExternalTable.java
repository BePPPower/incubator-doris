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

package org.apache.doris.catalog.external;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.trino.connector.TrinoConnectorExternalCatalog;
import org.apache.doris.thrift.THiveTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.trino.Session;
import io.trino.connector.CatalogName;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import static org.apache.doris.catalog.Column.COLUMN_UNIQUE_ID_INIT_VALUE;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;

public class TrinoConnectorExternalTable extends ExternalTable {

    private static final Logger LOG = LogManager.getLogger(TrinoConnectorExternalTable.class);

    public static final int TrinoConnector_DATETIME_SCALE_MS = 3;
    private Optional<ConnectorTableHandle> originTable = null;

    private Map<String, ColumnHandle> columnHandleMap = null;

    private Map<String, ColumnMetadata> columnMetadataMap = new HashMap<>();

    private Session trinoSession;

    public TrinoConnectorExternalTable(long id, String name, String dbName, TrinoConnectorExternalCatalog catalog) {
        super(id, name, catalog, dbName, TableType.TRINO_CONNECTOR_EXTERNAL_TABLE);
        trinoSession = catalog.getTrinoSession();
    }

    public String getTrinoConnectorCatalogType() {
        return ((TrinoConnectorExternalCatalog) catalog).getCatalogType();
    }

    protected synchronized void makeSureInitialized() {
        super.makeSureInitialized();
        if (!objectCreated) {
            objectCreated = true;
        }
    }

    public ConnectorTableHandle getOriginTable() {
        if (originTable == null) {
            originTable = ((TrinoConnectorExternalCatalog) catalog).getTrinoConnectorTable(dbName, name);
        }
        return originTable.get();
    }

    @Override
    public List<Column> initSchema() {
        Connector connector = ((TrinoConnectorExternalCatalog) catalog).getConnector();
        ConnectorTransactionHandle connectorTransactionHandle = connector.beginTransaction(
                IsolationLevel.READ_UNCOMMITTED, true, true);

        TableHandle tableHandle = getTrinoTableHandle(
                this.trinoSession, new QualifiedObjectName(catalog.getName(), dbName, name), connector, connectorTransactionHandle).get();
        columnHandleMap = new HashMap<>(getTrinoColumnHandles(this.trinoSession, tableHandle, connector, connectorTransactionHandle));
        List<Column> tmpSchema = Lists.newArrayListWithCapacity(columnHandleMap.size());
        for (Entry<String, ColumnHandle> entry : columnHandleMap.entrySet()) {
            ColumnHandle columnHandle = entry.getValue();
            ColumnMetadata columnMetadata = getTrinoColumnMetadata(this.trinoSession, tableHandle, columnHandle,
                    connector, connectorTransactionHandle);
            tmpSchema.add(new Column(columnMetadata.getName(),
                    TrinoConnectorTypeToDorisType(columnMetadata.getType()), true, null,
                    true, columnMetadata.getComment(), columnMetadata.isHidden(), COLUMN_UNIQUE_ID_INIT_VALUE));
            columnMetadataMap.put(columnMetadata.getName(), columnMetadata);
        }
        return tmpSchema;

        // chenqi

        // return localQueryRunner.inTransaction(transactionSession -> {
        //     TableHandle tableHandle = localQueryRunner.getMetadata().getTableHandle(transactionSession, new QualifiedObjectName(catalog.getName(), dbName, name)).get();
        //     columnHandleMap = new HashMap<>(localQueryRunner.getMetadata().getColumnHandles(transactionSession, tableHandle));
        //     List<Column> tmpSchema = Lists.newArrayListWithCapacity(columnHandleMap.size());
        //     for (Entry<String, ColumnHandle> entry : columnHandleMap.entrySet()) {
        //         ColumnHandle columnHandle = entry.getValue();
        //         ColumnMetadata columnMetadata = localQueryRunner.getMetadata().getColumnMetadata(transactionSession, tableHandle,
        //                 columnHandle);
        //         tmpSchema.add(new Column(columnMetadata.getName(),
        //                 TrinoConnectorTypeToDorisType(columnMetadata.getType()), true, null,
        //                 true, columnMetadata.getComment(), columnMetadata.isHidden(), COLUMN_UNIQUE_ID_INIT_VALUE));
        //         columnMetadataMap.put(columnMetadata.getName(), columnMetadata);
        //     }
        //     return tmpSchema;
        // });
    }

    private Optional<TableHandle> getTrinoTableHandle(Session session, QualifiedObjectName table, Connector connector, ConnectorTransactionHandle connectorTransactionHandle) {
        Objects.requireNonNull(table, "table is null");

        if (!table.getCatalogName().isEmpty()
                && !table.getSchemaName().isEmpty()
                && !table.getObjectName().isEmpty()) {
            CatalogName catalogName = ((TrinoConnectorExternalCatalog) catalog).getTrinoCatalogName();
            ConnectorSession connectorSession = session.toConnectorSession(catalogName);

            ConnectorMetadata connectorMetadata = connector.getMetadata(connectorSession, connectorTransactionHandle);
            return Optional.ofNullable(connectorMetadata.getTableHandle(connectorSession, table.asSchemaTableName()))
                    .map((connectorTableHandle) -> new TableHandle(catalogName, connectorTableHandle, connectorTransactionHandle));
        }
        return Optional.empty();
    }

    private Map<String, ColumnHandle> getTrinoColumnHandles(Session session, TableHandle tableHandle, Connector connector, ConnectorTransactionHandle connectorTransactionHandle) {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        ConnectorMetadata connectorMetadata = connector.getMetadata(connectorSession, connectorTransactionHandle);

        Map<String, ColumnHandle> handles = connectorMetadata.getColumnHandles(connectorSession, tableHandle.getConnectorHandle());
        ImmutableMap.Builder<String, ColumnHandle> map = ImmutableMap.builder();
        Iterator var7 = handles.entrySet().iterator();

        while(var7.hasNext()) {
            Map.Entry<String, ColumnHandle> mapEntry = (Map.Entry)var7.next();
            map.put(((String)mapEntry.getKey()).toLowerCase(Locale.ENGLISH), (ColumnHandle)mapEntry.getValue());
        }

        return map.buildOrThrow();
    }

    private ColumnMetadata getTrinoColumnMetadata(Session session, TableHandle tableHandle, ColumnHandle columnHandle,
                                        Connector connector, ConnectorTransactionHandle connectorTransactionHandle) {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        ConnectorMetadata connectorMetadata = connector.getMetadata(connectorSession, connectorTransactionHandle);
        return connectorMetadata.getColumnMetadata(connectorSession, tableHandle.getConnectorHandle(), columnHandle);
    }


    private Type TrinoConnectorPrimitiveTypeToDorisType(io.trino.spi.type.Type type) {
        if (type instanceof BooleanType) {
            return Type.BOOLEAN;
        } else if (type instanceof IntegerType) {
            return Type.INT;
        } else if (type instanceof BigintType) {
            return Type.BIGINT;
        // } else if (type instanceof FloatType) {
        //     return Type.FLOAT;
        } else if (type instanceof IntegerType) {
            return Type.DOUBLE;
        } else if (type instanceof SmallintType) {
            return Type.SMALLINT;
        } else if (type instanceof TinyintType) {
            return Type.TINYINT;
        } else if (type instanceof VarcharType) {
            return Type.STRING;
        // } else if (type instanceof BinaryType) {
        //     return Type.STRING;
        } else if (type instanceof CharType) {
            return Type.CHAR;
        } else if (type instanceof VarbinaryType) {
            return Type.STRING;
        } else if (type instanceof DecimalType) {
            DecimalType decimal = (DecimalType) type;
            return ScalarType.createDecimalV3Type(decimal.getPrecision(), decimal.getScale());
        } else if (type instanceof DateType) {
            return ScalarType.createDateV2Type();
        } else if (type instanceof TimestampType) {
            TimestampType timestampType = (TimestampType) type;
            return ScalarType.createDatetimeV2Type(timestampType.getPrecision());
        } else if (type instanceof TimestampWithTimeZoneType) {
            TimestampWithTimeZoneType timestampWithTimeZoneType = (TimestampWithTimeZoneType) type;
            return ScalarType.createDatetimeV2Type(timestampWithTimeZoneType.getPrecision());
        } else {
            throw new IllegalArgumentException("Cannot transform unknown type: " + type);
        }
    }

    protected Type TrinoConnectorTypeToDorisType(io.trino.spi.type.Type type) {
        return TrinoConnectorPrimitiveTypeToDorisType(type);
    }

    @Override
    public TTableDescriptor toThrift() {
        List<Column> schema = getFullSchema();
        if (TrinoConnectorExternalCatalog.TRINO_CONNECTOR_HMS.equals(getTrinoConnectorCatalogType()) || TrinoConnectorExternalCatalog.TRINO_CONNECTOR_FILESYSTEM
                .equals(getTrinoConnectorCatalogType())) {
            THiveTable tHiveTable = new THiveTable(dbName, name, new HashMap<>());
            TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.HIVE_TABLE, schema.size(), 0,
                    getName(), dbName);
            tTableDescriptor.setHiveTable(tHiveTable);
            return tTableDescriptor;
        } else {
            throw new IllegalArgumentException("Currently only supports hms/filesystem catalog,not support :"
                    + getTrinoConnectorCatalogType());
        }
    }

    public Map<String, ColumnHandle> getColumnHandleMap() {
        return columnHandleMap;
    }

    public Map<String, ColumnMetadata> getColumnMetadataMap() {
        return columnMetadataMap;
    }

    public Session getTrinoSession() {
        return trinoSession;
    }
}
