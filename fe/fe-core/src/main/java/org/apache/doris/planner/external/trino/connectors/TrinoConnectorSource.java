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

package org.apache.doris.planner.external.trino.connectors;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.external.TrinoConnectorExternalTable;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.trino.connector.TrinoConnectorExternalCatalog;
import org.apache.doris.thrift.TFileAttributes;

import io.trino.Session;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorTransactionHandle;

public class TrinoConnectorSource {
    private final TrinoConnectorExternalTable trinoConnectorExtTable;

    private final TableHandle trinoConnectorExtTableHandle;

    private final TupleDescriptor desc;

    private final Session trinoSession;

    private final Connector connector;

    private ConnectorTransactionHandle connectorTransactionHandle;

    public TrinoConnectorSource(TrinoConnectorExternalTable table, TupleDescriptor desc) {
        this.trinoConnectorExtTable = table;
        this.trinoConnectorExtTableHandle = trinoConnectorExtTable.getOriginTable();
        this.desc = desc;

        this.trinoSession = trinoConnectorExtTable.getTrinoSession();
        this.connector = ((TrinoConnectorExternalCatalog) trinoConnectorExtTable.getCatalog()).getConnector();
    }

    public TupleDescriptor getDesc() {
        return desc;
    }

    public TableHandle getTrinoConnectorExtTableHandle() {
        return trinoConnectorExtTableHandle;
    }

    public TableIf getTargetTable() {
        return trinoConnectorExtTable;
    }

    public TFileAttributes getFileAttributes() throws UserException {
        return new TFileAttributes();
    }

    public ExternalCatalog getCatalog() {
        return trinoConnectorExtTable.getCatalog();
    }

    public Session getTrinoSession() {
        return trinoSession;
    }

    public Connector getConnector() {
        return connector;
    }

    public void setConnectorTransactionHandle(ConnectorTransactionHandle connectorTransactionHandle) {
        this.connectorTransactionHandle = connectorTransactionHandle;
    }

    public ConnectorTransactionHandle getConnectorTransactionHandle() {
        return connectorTransactionHandle;
    }
}
