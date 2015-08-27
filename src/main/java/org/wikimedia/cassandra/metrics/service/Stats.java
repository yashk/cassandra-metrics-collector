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

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

class Stats {

    private final Map<String, Integer> successes = Maps.newHashMap();
    private final Map<String, Integer> failures = Maps.newHashMap();

    Collection<String> getNames() {
        Set<String> names = Sets.newHashSet(this.successes.keySet());
        names.addAll(this.failures.keySet());
        return names;
    }

    int getSuccesses(String name) {
        return get(name, this.successes);
    }

    int getFailures(String name) {
        return get(name, this.failures);
    }

    synchronized void success(String name) {
        increment(name, this.successes);
    }

    synchronized void failure(String name) {
        increment(name, this.failures);
    }

    private Integer get(String name, Map<String, Integer> map) {
        Integer count = map.get(name);
        return count == null ? 0 : count; 
    }

    private static void increment(String name, Map<String, Integer> map) {
        Integer count = map.get(name);
        map.put(name, incrCounter(count == null ? 0 : count));
    }

    private static int incrCounter(int v) {
        return (v < Integer.MAX_VALUE) ? v + 1 : 0;
    }

}
