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

import java.util.List;
import java.util.regex.Pattern;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class Filter {

    private List<Pattern> blacklist;
    
    public Filter(FilterConfig config) {
        blacklist = Lists.newArrayList(
            Iterables.transform(config.getBlacklist(), new Function<String, Pattern>() {
                @Override
                public Pattern apply(String input) {
                     return Pattern.compile(input);
                }
            })
        );

    }

    private boolean blackListed(String id) {
        for (Pattern p : blacklist) {
            if (p.matcher(id).matches()) {
                return true;
            }
        }
        return false;
    }

    public boolean accept(String id) {
        return !blackListed(id);
    }

    @Override
    public String toString() {
        return "Filter [blacklist=" + blacklist + "]";
    }

}
