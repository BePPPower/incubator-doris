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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.trino.connector.CatalogName;
import io.trino.metadata.HandleResolver;
import io.trino.metadata.TypeRegistry;
import io.trino.server.PluginClassLoader;
import io.trino.server.PluginManager.PluginsProvider;
import io.trino.spi.Plugin;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

@ThreadSafe
public class TrinoConnectorPluginManager {
    private static final Logger LOG = LogManager.getLogger(TrinoConnectorPluginManager.class);
    private static final ImmutableList<String> SPI_PACKAGES = ImmutableList.of("io.trino.spi.",
            "com.fasterxml.jackson.annotation.",
            "io.airlift.slice.",
            "org.openjdk.jol.");

    private final ConcurrentMap<String, TrinoConnectorInternalConnectorFactory> connectorFactories = new ConcurrentHashMap();
    private final PluginsProvider pluginsProvider;
    private final TypeRegistry typeRegistry;
    private final HandleResolver handleResolver;
    private final AtomicBoolean pluginsLoading = new AtomicBoolean();

    @Inject
    public TrinoConnectorPluginManager(PluginsProvider pluginsProvider, TypeRegistry typeRegistry, HandleResolver handleResolver) {
        this.pluginsProvider = Objects.requireNonNull(pluginsProvider, "pluginsProvider is null");
        this.typeRegistry = Objects.requireNonNull(typeRegistry, "typeRegistry is null");
        this.handleResolver = Objects.requireNonNull(handleResolver, "handleResolver is null");
    }

    public void loadPlugins() {
        if (!pluginsLoading.compareAndSet(false, true)) {
            return;
        }

        pluginsProvider.loadPlugins(this::loadPlugin, TrinoConnectorPluginManager::createClassLoader);

        typeRegistry.verifyTypes();
    }

    private void loadPlugin(String plugin, Supplier<PluginClassLoader> createClassLoader) {
        LOG.info("-- Loading plugin %s --", new Object[]{plugin});

        PluginClassLoader pluginClassLoader = createClassLoader.get();

        LOG.debug("Classpath for plugin:");
        for (URL url : pluginClassLoader.getURLs()) {
            LOG.debug("    %s", url.getPath());
        }

        handleResolver.registerClassLoader(pluginClassLoader);
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(pluginClassLoader)) {
            loadPlugin(pluginClassLoader);
        }

        LOG.info("-- Finished loading plugin %s --", plugin);
    }

    private void loadPlugin(PluginClassLoader pluginClassLoader) {
        ServiceLoader<Plugin> serviceLoader = ServiceLoader.load(Plugin.class, pluginClassLoader);
        List<Plugin> plugins = ImmutableList.copyOf(serviceLoader);
        Preconditions.checkState(!plugins.isEmpty(), "No service providers of type %s in the classpath: %s", Plugin.class.getName(), Arrays.asList(pluginClassLoader.getURLs()));

        for (Plugin plugin : plugins) {
            LOG.info("Installing %s", plugin.getClass().getName());
            installPlugin(plugin, pluginClassLoader::duplicate);
        }
    }

    public void installPlugin(Plugin plugin, Function<CatalogName, ClassLoader> duplicatePluginClassLoaderFactory) {
        this.installPluginInternal(plugin, duplicatePluginClassLoaderFactory);
        this.typeRegistry.verifyTypes();
    }

    private void installPluginInternal(Plugin plugin, Function<CatalogName, ClassLoader> duplicatePluginClassLoaderFactory) {
        for (ConnectorFactory connectorFactory : plugin.getConnectorFactories()) {
            LOG.info("Registering connector %s", connectorFactory.getName());
            TrinoConnectorInternalConnectorFactory existingConnectorFactory = this.connectorFactories.putIfAbsent(
                    connectorFactory.getName(), new TrinoConnectorInternalConnectorFactory(connectorFactory, duplicatePluginClassLoaderFactory));
            Preconditions.checkArgument(existingConnectorFactory == null, "Connector '%s' is already registered", connectorFactory.getName());
        }
    }

    public static PluginClassLoader createClassLoader(String pluginName, List<URL> urls) {
        ClassLoader parent = TrinoConnectorPluginManager.class.getClassLoader();
        return new PluginClassLoader(pluginName, urls, parent, SPI_PACKAGES);
    }

    public ConcurrentMap<String, TrinoConnectorInternalConnectorFactory> getConnectorFactories() {
        return connectorFactories;
    }

    public HandleResolver getHandleResolver() {
        return handleResolver;
    }

    public TypeRegistry getTypeRegistry() {
        return typeRegistry;
    }
}
