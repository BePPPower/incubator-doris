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

import org.apache.doris.trino.connector.TrinoConnectorPluginManager.PluginsProvider;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import io.trino.server.ForStartup;
import io.trino.util.Executors;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import javax.inject.Inject;

public class TrinoConnectorServerPluginsProvider implements PluginsProvider {
    private final File installedPluginsDir;
    private final Executor executor;

    @Inject
    public TrinoConnectorServerPluginsProvider(TrinoConnectorServerPluginsProviderConfig config, @ForStartup Executor executor) {
        this.installedPluginsDir = config.getInstalledPluginsDir();
        this.executor = (Executor) Objects.requireNonNull(executor, "executor is null");
    }

    public void loadPlugins(PluginsProvider.Loader loader, PluginsProvider.ClassLoaderFactory createClassLoader) {
        Executors.executeUntilFailure(this.executor, (Collection)listFiles(this.installedPluginsDir).stream().filter(File::isDirectory).map((file) -> {
            return new Callable<Object>() {
                @Override
                public Object call() throws Exception{
                    loader.load(file.getAbsolutePath(), () -> {
                        return createClassLoader.create(file.getName(), buildClassPath(file));
                    });
                    return null;
                }
            };
        }).collect(ImmutableList.toImmutableList()));
    }

    private static List<URL> buildClassPath(File path) {
        return (List)listFiles(path).stream().map(TrinoConnectorServerPluginsProvider::fileToUrl).collect(ImmutableList.toImmutableList());
    }

    private static List<File> listFiles(File path) {
        try {
            DirectoryStream<Path> directoryStream = Files.newDirectoryStream(path.toPath());

            List var2;
            try {
                var2 = (List) Streams.stream(directoryStream).map(Path::toFile).sorted().collect(ImmutableList.toImmutableList());
            } catch (Throwable var5) {
                if (directoryStream != null) {
                    try {
                        directoryStream.close();
                    } catch (Throwable var4) {
                        var5.addSuppressed(var4);
                    }
                }

                throw var5;
            }

            if (directoryStream != null) {
                directoryStream.close();
            }

            return var2;
        } catch (IOException var6) {
            throw new UncheckedIOException(var6);
        }
    }

    private static URL fileToUrl(File file) {
        try {
            return file.toURI().toURL();
        } catch (MalformedURLException var2) {
            throw new UncheckedIOException(var2);
        }
    }
}
