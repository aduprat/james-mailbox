/****************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one   *
 * or more contributor license agreements.  See the NOTICE file *
 * distributed with this work for additional information        *
 * regarding copyright ownership.  The ASF licenses this file   *
 * to you under the Apache License, Version 2.0 (the            *
 * "License"); you may not use this file except in compliance   *
 * with the License.  You may obtain a copy of the License at   *
 *                                                              *
 *   http://www.apache.org/licenses/LICENSE-2.0                 *
 *                                                              *
 * Unless required by applicable law or agreed to in writing,   *
 * software distributed under the License is distributed on an  *
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY       *
 * KIND, either express or implied.  See the License for the    *
 * specific language governing permissions and limitations      *
 * under the License.                                           *
 ****************************************************************/
package org.apache.james.mailbox.cassandra;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import com.datastax.driver.core.Cluster;

@Singleton
public class ClusterProvider implements Provider<Cluster> {

    private final String ip;
    private final int port;
    private final String keyspace;
    private final int replicationFactor;

    @Inject
    private ClusterProvider(@Named("cassandra.ip") String ip, @Named("cassandra.port") int port,
            @Named("cassandra.keyspace") String keyspace, @Named("cassandra.replication.factor") int replicationFactor) {

        this.ip = ip;
        this.port = port;
        this.keyspace = keyspace;
        this.replicationFactor = replicationFactor;
        
    }

    @Override
    public Cluster get() {
        return ClusterWithKeyspaceCreatedFactory.clusterWithInitializedKeyspace(
                ClusterFactory.createClusterForSingleServerWithoutPassWord(ip, port),
                keyspace, replicationFactor);
    }
}