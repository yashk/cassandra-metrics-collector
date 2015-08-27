/* Copyright 2015 Eric Evans <eevans@wikimedia.org> and Wikimedia Foundation
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.wikimedia.cassandra.metrics.service;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.wikimedia.cassandra.metrics.Discovery;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

class InstanceCache {
    private Map<String, Discovery.Jvm> cache = new HashMap<>();

    Collection<Discovery.Jvm> discover() throws IOException {
        Map<String, Discovery.Jvm> discovered = new Discovery().getJvms();
        Set<Entry<String, Discovery.Jvm>> toAdd = Sets.newHashSet(discovered.entrySet());

        toAdd.removeAll(cache.entrySet());

        // Return only the delta.
        List<Discovery.Jvm> r = Lists.newArrayList();

        for (Entry<String, Discovery.Jvm> e : toAdd)
            r.add(e.getValue());

        return r;
    }

    void add(String instanceName, Discovery.Jvm vm) {
        this.cache.put(instanceName, vm);
    }

    void remove(String instanceName) {
        this.cache.remove(instanceName);
    }

}
