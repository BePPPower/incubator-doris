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

import static com.google.common.base.Preconditions.checkState;
import io.trino.connector.CatalogName;
import io.trino.metadata.HandleResolver;
import io.trino.server.PluginClassLoader;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.concurrent.GuardedBy;

public class TrinoConnectorCatalogClassLoaderSupplier implements Supplier<ClassLoader>  {
    private final CatalogName catalogName;
    private final Function<CatalogName, ClassLoader> duplicatePluginClassLoaderFactory;
    private final HandleResolver handleResolver;
    @GuardedBy("this")
    private boolean destroyed;
    @GuardedBy("this")
    private ClassLoader classLoader;

    public TrinoConnectorCatalogClassLoaderSupplier(CatalogName catalogName, Function<CatalogName, ClassLoader> duplicatePluginClassLoaderFactory, HandleResolver handleResolver) {
        this.catalogName = Objects.requireNonNull(catalogName, "catalogName is null");
        this.duplicatePluginClassLoaderFactory = Objects.requireNonNull(duplicatePluginClassLoaderFactory, "duplicatePluginClassLoaderFactory is null");
        this.handleResolver = Objects.requireNonNull(handleResolver, "handleResolver is null");
    }

    public ClassLoader get() {
        ClassLoader classLoader = this.duplicatePluginClassLoaderFactory.apply(this.catalogName);
        synchronized (this) {
            // we check this after class loader creation because it reduces the complexity of the synchronization, and this shouldn't happen
            checkState(this.classLoader == null, "class loader is already a duplicated for catalog " + catalogName);
            checkState(!destroyed, "catalog has been shutdown");
            this.classLoader = classLoader;
        }

        if (classLoader instanceof PluginClassLoader) {
            handleResolver.registerClassLoader((PluginClassLoader) classLoader);
        }
        return classLoader;
    }

    public void destroy() {
        ClassLoader classLoader;
        synchronized (this) {
            checkState(!destroyed, "catalog has been shutdown");
            classLoader = this.classLoader;
            destroyed = true;
        }

        if (classLoader instanceof PluginClassLoader) {
            this.handleResolver.unregisterClassLoader((PluginClassLoader)classLoader);
        }

    }
}
