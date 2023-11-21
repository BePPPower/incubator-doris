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

package org.apache.doris.datasource.trino.connector.TrinoConnectorManagers.TrinoConnectorConnectorManager;

import com.google.common.base.Preconditions;
import io.trino.connector.CatalogName;
import io.trino.connector.ConnectorAwareNodeManager;
import io.trino.metadata.InMemoryNodeManager;
import io.trino.operator.GroupByHashPageIndexerFactory;
import io.trino.operator.PagesIndex;
import io.trino.operator.PagesIndexPageSorter;
import io.trino.spi.NodeManager;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.PageSorter;
import io.trino.spi.VersionEmbedder;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.MetadataProvider;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.JoinCompiler;
import io.trino.type.BlockTypeOperators;
import io.trino.type.InternalTypeManager;
import io.trino.version.EmbedVersion;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public final class TrinoConenctorTestingConnectorContext implements ConnectorContext {
    private final NodeManager nodeManager;
    private final VersionEmbedder versionEmbedder = new EmbedVersion("testversion");
    private final PageSorter pageSorter = new PagesIndexPageSorter(new PagesIndex.TestingFactory(false));
    private final PageIndexerFactory pageIndexerFactory;
    private final Supplier<ClassLoader> duplicatePluginClassLoaderFactory;
    private final AtomicBoolean pluginClassLoaderDuplicated = new AtomicBoolean();

    public TrinoConenctorTestingConnectorContext(Supplier<ClassLoader> duplicatePluginClassLoaderFactory) {
        TypeOperators typeOperators = new TypeOperators();
        this.pageIndexerFactory = new GroupByHashPageIndexerFactory(new JoinCompiler(typeOperators), new BlockTypeOperators(typeOperators));
        CatalogName catalogName = new CatalogName("test");
        InMemoryNodeManager inMemoryNodeManager = new InMemoryNodeManager();
        inMemoryNodeManager.addCurrentNodeConnector(catalogName);
        this.nodeManager = new ConnectorAwareNodeManager(inMemoryNodeManager, "testenv", catalogName, true);
        this.duplicatePluginClassLoaderFactory = (Supplier) Objects.requireNonNull(duplicatePluginClassLoaderFactory, "duplicatePluginClassLoaderFactory is null");
    }

    public NodeManager getNodeManager() {
        return this.nodeManager;
    }

    public VersionEmbedder getVersionEmbedder() {
        return this.versionEmbedder;
    }

    public TypeManager getTypeManager() {
        return InternalTypeManager.TESTING_TYPE_MANAGER;
    }

    public MetadataProvider getMetadataProvider() {
        return MetadataProvider.NOOP_METADATA_PROVIDER;
    }

    public PageSorter getPageSorter() {
        return this.pageSorter;
    }

    public PageIndexerFactory getPageIndexerFactory() {
        return this.pageIndexerFactory;
    }

    public ClassLoader duplicatePluginClassLoader() {
        Preconditions.checkState(!this.pluginClassLoaderDuplicated.getAndSet(true), "plugin class loader already duplicated");
        return (ClassLoader)this.duplicatePluginClassLoaderFactory.get();
    }
}
