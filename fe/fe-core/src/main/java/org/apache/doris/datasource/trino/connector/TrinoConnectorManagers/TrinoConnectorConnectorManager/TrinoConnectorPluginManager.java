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
import com.google.common.collect.ImmutableList;
import io.trino.connector.CatalogName;
import io.trino.metadata.HandleResolver;
import io.trino.metadata.TypeRegistry;
import io.trino.server.PluginClassLoader;
import io.trino.spi.Plugin;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URL;
import java.util.Arrays;
import java.util.Iterator;
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
    private final AtomicBoolean pluginsLoaded = new AtomicBoolean();

    @Inject
    public TrinoConnectorPluginManager(PluginsProvider pluginsProvider, TypeRegistry typeRegistry, HandleResolver handleResolver) {
        this.pluginsProvider = (PluginsProvider) Objects.requireNonNull(pluginsProvider, "pluginsProvider is null");
        this.typeRegistry = (TypeRegistry)Objects.requireNonNull(typeRegistry, "typeRegistry is null");
        this.handleResolver = (HandleResolver)Objects.requireNonNull(handleResolver, "handleResolver is null");
    }

    public void loadPlugins() {
        if (this.pluginsLoading.compareAndSet(false, true)) {
            this.pluginsProvider.loadPlugins(this::loadPlugin, TrinoConnectorPluginManager::createClassLoader);
            this.typeRegistry.verifyTypes();
            this.pluginsLoaded.set(true);
        }
    }

    private void loadPlugin(String plugin, Supplier<PluginClassLoader> createClassLoader) {
        LOG.info("-- Loading plugin %s --", new Object[]{plugin});
        PluginClassLoader pluginClassLoader = (PluginClassLoader)createClassLoader.get();
        LOG.debug("Classpath for plugin:");
        URL[] var4 = pluginClassLoader.getURLs();
        int var5 = var4.length;

        for(int var6 = 0; var6 < var5; ++var6) {
            URL url = var4[var6];
            LOG.debug("    %s", new Object[]{url.getPath()});
        }

        this.handleResolver.registerClassLoader(pluginClassLoader);
        ThreadContextClassLoader ignored = new ThreadContextClassLoader(pluginClassLoader);

        try {
            this.loadPlugin(pluginClassLoader);
        } catch (Throwable var9) {
            try {
                ignored.close();
            } catch (Throwable var8) {
                var9.addSuppressed(var8);
            }

            throw var9;
        }

        ignored.close();
        LOG.info("-- Finished loading plugin %s --", new Object[]{plugin});
    }

    private void loadPlugin(PluginClassLoader pluginClassLoader) {
        ServiceLoader<Plugin> serviceLoader = ServiceLoader.load(Plugin.class, pluginClassLoader);
        List<Plugin> plugins = ImmutableList.copyOf(serviceLoader);
        Preconditions.checkState(!plugins.isEmpty(), "No service providers of type %s in the classpath: %s", Plugin.class.getName(), Arrays.asList(pluginClassLoader.getURLs()));
        Iterator var4 = plugins.iterator();

        while(var4.hasNext()) {
            Plugin plugin = (Plugin)var4.next();
            LOG.info("Installing %s", new Object[]{plugin.getClass().getName()});
            Objects.requireNonNull(pluginClassLoader);
            this.installPlugin(plugin, pluginClassLoader::duplicate);
        }

    }

    public void installPlugin(Plugin plugin, Function<CatalogName, ClassLoader> duplicatePluginClassLoaderFactory) {
        this.installPluginInternal(plugin, duplicatePluginClassLoaderFactory);
        this.typeRegistry.verifyTypes();
    }

    private void installPluginInternal(Plugin plugin, Function<CatalogName, ClassLoader> duplicatePluginClassLoaderFactory) {
        Iterator var3 = plugin.getConnectorFactories().iterator();

        while(var3.hasNext()) {
            ConnectorFactory connectorFactory = (ConnectorFactory)var3.next();
            LOG.info("Registering connector %s", new Object[]{connectorFactory.getName()});
            Objects.requireNonNull(connectorFactory, "connectorFactory is null");
            Objects.requireNonNull(duplicatePluginClassLoaderFactory, "duplicatePluginClassLoaderFactory is null");
            TrinoConnectorInternalConnectorFactory existingConnectorFactory = (TrinoConnectorInternalConnectorFactory)this.connectorFactories.putIfAbsent(
                    connectorFactory.getName(), new TrinoConnectorInternalConnectorFactory(connectorFactory, duplicatePluginClassLoaderFactory));
            Preconditions.checkArgument(existingConnectorFactory == null, "Connector '%s' is already registered", connectorFactory.getName());
        }
    }

    public static PluginClassLoader createClassLoader(String pluginName, List<URL> urls) {
        ClassLoader parent = TrinoConnectorPluginManager.class.getClassLoader();
        return new PluginClassLoader(pluginName, urls, parent, SPI_PACKAGES);
    }

    public interface PluginsProvider {
        void loadPlugins(Loader loader, ClassLoaderFactory createClassLoader);

        public interface ClassLoaderFactory {
            PluginClassLoader create(String pluginName, List<URL> urls);
        }

        public interface Loader {
            void load(String description, Supplier<PluginClassLoader> getClassLoader);
        }
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
