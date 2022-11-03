/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.service.plugin;

import lombok.Data;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.common.api.proto.MessageMetadata;

@Data
public class FilterContext {
    private Subscription subscription;
    private MessageMetadata msgMetadata;
    private Consumer consumer;

    public void reset() {
        subscription = null;
        msgMetadata = null;
        consumer = null;
    }

    public void fill(Subscription subscription, MessageMetadata msgMetadata, Consumer consumer) {
        this.subscription = subscription;
        this.msgMetadata = msgMetadata;
        this.consumer = consumer;
    }

    public static final FilterContext FILTER_CONTEXT_DISABLED = new FilterContext();
}
