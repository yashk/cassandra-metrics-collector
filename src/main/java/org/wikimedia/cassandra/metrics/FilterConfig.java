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
package org.wikimedia.cassandra.metrics;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@ConfigurationProperties(prefix="config.filterConfig")
public class FilterConfig {
    private List<String> whitelist = new ArrayList<>();
    private List<String> blacklist = new ArrayList<>();

    public List<String> getBlacklist() {
        return (blacklist != null) ? blacklist : Collections.<String>emptyList();
    }

    public void setBlacklist(List<String> blacklist) {
        this.blacklist = blacklist;
    }

    public Collection<String> getWhitelist() {
        return (whitelist != null) ? whitelist : Collections.<String>emptyList();
    }

    public void setWhitelist(List<String> whitelist) {
        this.whitelist = whitelist;
    }

    @Override
    public String toString() {
        return "FilterConfig [blacklist=" + blacklist + ", whitelist=" + whitelist + "]";
    }

}
