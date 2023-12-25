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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.trino.FeaturesConfig;
import io.trino.connector.CatalogName;
import io.trino.connector.ConnectorAwareNodeManager;
import io.trino.connector.ConnectorContextInstance;
import io.trino.connector.InternalMetadataProvider;
import io.trino.metadata.HandleResolver;
import io.trino.metadata.InMemoryNodeManager;
import io.trino.metadata.MetadataManager;
import io.trino.operator.GroupByHashPageIndexerFactory;
import io.trino.operator.PagesIndex;
import io.trino.operator.PagesIndexPageSorter;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.JoinCompiler;
import io.trino.type.BlockTypeOperators;
import io.trino.type.InternalTypeManager;
import io.trino.version.EmbedVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Used to cache connector
 */
public class TrinoConnectorCache {
    private static final Logger LOG = LoggerFactory.getLogger(TrinoConnectorCache.class);

    // Max cache num of trino connector
    public static final long max_external_schema_cache_num = 100;

    private static LoadingCache<TrinoConnectorCacheKey, TrinoConnectorCacheValue> connectorCache = CacheBuilder.newBuilder()
            .maximumSize(max_external_schema_cache_num)
            .build(new CacheLoader<TrinoConnectorCacheKey, TrinoConnectorCacheValue>() {
                @Override
                public TrinoConnectorCacheValue load(TrinoConnectorCacheKey key) throws Exception {
                    return loadCache(key);
                }
            });

    public static void invalidateTableCache(TrinoConnectorCacheKey key) {
        try {
            TrinoConnectorCacheValue connectorCacheValue = connectorCache.get(key);

            Connector connector = connectorCacheValue.getConnector();
            TrinoConnectorCatalogClassLoaderSupplier duplicatePluginClassLoaderFactory
                    = connectorCacheValue.getDuplicatePluginClassLoaderFactory();
            if (connector != null) {
                try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(connector.getClass().getClassLoader())) {
                    connector.shutdown();
                } finally {
                    duplicatePluginClassLoaderFactory.destroy();
                }
            }

            connectorCache.invalidate(key);
        } catch (Exception e) {
            throw new RuntimeException("failed to invalidate connector for:" + key);
        }
    }

    public static TrinoConnectorCacheValue getConnector(TrinoConnectorCacheKey key) {
        try {
            return connectorCache.get(key);
        } catch (Exception e) {
            throw new RuntimeException("failed to get connector for:" + key);
        }
    }

    private static TrinoConnectorCacheValue loadCache(TrinoConnectorCacheKey key) {
        try {
            LOG.info("load connector:{}", key);
            HandleResolver handleResolver = new HandleResolver();
            TrinoConnectorInternalConnectorFactory connectorFactory = key.trinoConnectorPluginManager
                    .getConnectorFactories().get(key.connectorName);
            TrinoConnectorCatalogClassLoaderSupplier duplicatePluginClassLoaderFactory = new TrinoConnectorCatalogClassLoaderSupplier(
                    key.trinoCatalogName, connectorFactory.getDuplicatePluginClassLoaderFactory(), handleResolver);

            Connector connector = createConnector(key.trinoCatalogName, connectorFactory.getConnectorFactory(),
                    duplicatePluginClassLoaderFactory, key.properties, key.trinoConnectorPluginManager, key.featuresConfig);
            return new TrinoConnectorCacheValue(connector, handleResolver, duplicatePluginClassLoaderFactory);
        } catch (Exception e) {
            LOG.warn("failed to create trino connector", e);
            throw new RuntimeException(e);
        }
    }


    private static Connector createConnector(
            CatalogName catalogName, ConnectorFactory connectorFactory,
            Supplier<ClassLoader> duplicatePluginClassLoaderFactory, Map<String, String> properties,
            TrinoConnectorPluginManager trinoConnectorPluginManager, FeaturesConfig featuresConfig) {
        InMemoryNodeManager inMemoryNodeManager = new InMemoryNodeManager();
        inMemoryNodeManager.addCurrentNodeConnector(catalogName);
        TypeManager typeManager = new InternalTypeManager(trinoConnectorPluginManager.getTypeRegistry());
        TypeOperators typeOperators = trinoConnectorPluginManager.getTypeRegistry().getTypeOperators();
        ConnectorContext context = new ConnectorContextInstance(
                new ConnectorAwareNodeManager(inMemoryNodeManager, "testenv", catalogName, true),
                EmbedVersion.testingVersionEmbedder(),
                typeManager,
                new InternalMetadataProvider(MetadataManager.createTestMetadataManager(featuresConfig), typeManager),
                new PagesIndexPageSorter(new PagesIndex.TestingFactory(false)),
                new GroupByHashPageIndexerFactory(new JoinCompiler(typeOperators), new BlockTypeOperators(typeOperators)),
                duplicatePluginClassLoaderFactory);

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(connectorFactory.getClass().getClassLoader())) {
            return connectorFactory.create(catalogName.getCatalogName(), properties, context);
        }
    }

    public static class TrinoConnectorCacheKey {
        private String catalogName;
        private String connectorName;

        // other properties
        private CatalogName trinoCatalogName;
        private Map<String, String> properties;

        private TrinoConnectorPluginManager trinoConnectorPluginManager;
        private FeaturesConfig featuresConfig;

        public TrinoConnectorCacheKey(String catalogName, String connectorName) {
            this.catalogName = catalogName;
            this.connectorName = connectorName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TrinoConnectorCacheKey that = (TrinoConnectorCacheKey) o;
            return Objects.equals(catalogName, that.catalogName) && Objects.equals(connectorName, that.connectorName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(catalogName, connectorName);
        }

        @Override
        public String toString() {
            return "TrinoConnectorCacheKey{" + "catalogName='" + catalogName + '\'' + ", connectorName='"
                    + connectorName + '\'' + '}';
        }

        public void setTrinoCatalogName(CatalogName trinoCatalogName) {
            this.trinoCatalogName = trinoCatalogName;
        }

        public void setProperties(Map<String, String> properties) {
            this.properties = properties;
        }

        public void setTrinoConnectorPluginManager(TrinoConnectorPluginManager trinoConnectorPluginManager) {
            this.trinoConnectorPluginManager = trinoConnectorPluginManager;
        }

        public void setFeaturesConfig(FeaturesConfig featuresConfig) {
            this.featuresConfig = featuresConfig;
        }
    }

    public static class TrinoConnectorCacheValue {
        private Connector connector;
        private HandleResolver handleResolver;
        private TrinoConnectorCatalogClassLoaderSupplier duplicatePluginClassLoaderFactory;

        public TrinoConnectorCacheValue(Connector connector, HandleResolver handleResolver,
                TrinoConnectorCatalogClassLoaderSupplier duplicatePluginClassLoaderFactory) {
            this.connector = connector;
            this.handleResolver = handleResolver;
            this.duplicatePluginClassLoaderFactory = duplicatePluginClassLoaderFactory;
        }

        public void setConnector(Connector connector) {
            this.connector = connector;
        }

        public void setHandleResolver(HandleResolver handleResolver) {
            this.handleResolver = handleResolver;
        }

        public Connector getConnector() {
            return connector;
        }

        public HandleResolver getHandleResolver() {
            return handleResolver;
        }

        public TrinoConnectorCatalogClassLoaderSupplier getDuplicatePluginClassLoaderFactory() {
            return duplicatePluginClassLoaderFactory;
        }
    }
}
