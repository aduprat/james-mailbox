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

import com.datastax.driver.core.Session;

@Singleton
public class SessionProvider implements Provider<Session> {

    private final ClusterProvider clusterProvider;
    private final String keyspace;

    @Inject
    private SessionProvider(ClusterProvider clusterProvider, @Named("cassandra.keyspace") String keyspace) {
        this.clusterProvider = clusterProvider;
        this.keyspace = keyspace;
        
    }

    @Override
    public Session get() {
        return SessionFactory.createSession(clusterProvider.get(), keyspace);
    }
}