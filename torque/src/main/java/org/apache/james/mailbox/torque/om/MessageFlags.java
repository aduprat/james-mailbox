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
package org.apache.james.mailbox.torque.om;

import javax.mail.Flags;

import org.apache.torque.om.Persistent;

/**
 * The skeleton for this class was autogenerated by Torque on:
 * 
 * [Wed Sep 06 19:48:03 CEST 2006]
 * 
 * You should add additional methods to this class to meet the application
 * requirements. This class will only be generated as long as it does not
 * already exist in the output directory.
 *
 * @deprecated Torque implementation will get removed in the next release
 */
@Deprecated()
public class MessageFlags extends
        org.apache.james.mailbox.torque.om.BaseMessageFlags implements
        Persistent {
    private static final long serialVersionUID = -7426028860085278304L;

    public void setFlags(Flags flags) {
        setAnswered(flags.contains(Flags.Flag.ANSWERED));
        setDeleted(flags.contains(Flags.Flag.DELETED));
        setDraft(flags.contains(Flags.Flag.DRAFT));
        setFlagged(flags.contains(Flags.Flag.FLAGGED));
        setRecent(flags.contains(Flags.Flag.RECENT));
        setSeen(flags.contains(Flags.Flag.SEEN));
    }

    public Flags createFlags() {
        Flags flags = new Flags();

        if (getAnswered()) {
            flags.add(Flags.Flag.ANSWERED);
        }
        if (getDeleted()) {
            flags.add(Flags.Flag.DELETED);
        }
        if (getDraft()) {
            flags.add(Flags.Flag.DRAFT);
        }
        if (getFlagged()) {
            flags.add(Flags.Flag.FLAGGED);
        }
        if (getRecent()) {
            flags.add(Flags.Flag.RECENT);
        }
        if (getSeen()) {
            flags.add(Flags.Flag.SEEN);
        }
        return flags;
    }
}
