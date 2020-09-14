// Copyright 2019 IBM
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudant.search3;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import java.io.File;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.INIConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Main {

  private static Logger LOGGER;

  public static void main(String[] args) throws Exception {
    final Configurations configurations = new Configurations();
    final INIConfiguration configuration = configurations.ini(new File("search3.ini"));
    final Configuration metricsConfiguration = configuration.configurationAt("metrics");
    final Configuration searchConfiguration = configuration.configurationAt("search");

    System.setProperty("java.util.logging.manager", "org.apache.logging.log4j.jul.LogManager");
    LOGGER = LogManager.getLogger();

    // Initialize FDB.
    FDB.selectAPIVersion(configuration.getInt("fdb.version"));
    Database DB = FDB.instance().open();
    byte[] prefix = new byte[]{1, 2, 3};
    Subspace subspace = new Subspace(prefix);
    final byte[] key = subspace.pack(Tuple.from("health", "check"));
    DB.run(
            txn -> {
              txn.get(key).join();
              return null;
            });
    LOGGER.info("Connected to FDB.");

    // Start metrics first.
    final MetricsServer metricsServer = new MetricsServer(metricsConfiguration);
    metricsServer.start();

    // Start search next.
    final Search search = new Search(searchConfiguration);
    final SearchServer searchServer = new SearchServer(searchConfiguration, search);
    searchServer.start();

    searchServer.stop();
    metricsServer.stop();

    LOGGER.info("Server terminated.");
  }
}
