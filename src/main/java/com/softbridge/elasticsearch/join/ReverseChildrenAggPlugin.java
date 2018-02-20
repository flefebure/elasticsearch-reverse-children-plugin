/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.softbridge.elasticsearch.join;

import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.join.aggregations.ChildrenAggregationBuilder;
import org.elasticsearch.join.aggregations.InternalChildren;
import org.elasticsearch.join.mapper.ParentJoinFieldMapper;
import org.elasticsearch.join.query.HasChildQueryBuilder;
import org.elasticsearch.join.query.HasParentQueryBuilder;
import org.elasticsearch.join.query.ParentIdQueryBuilder;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ReverseChildrenAggPlugin extends Plugin implements SearchPlugin, MapperPlugin {

    public ReverseChildrenAggPlugin() {
    }

    @Override
    public List<AggregationSpec> getAggregations() {
        return Arrays.asList(
            new AggregationSpec(ReverseChildrenAggregationBuilder.NAME,
                    ReverseChildrenAggregationBuilder::new,
                    ReverseChildrenAggregationBuilder::parse)
                .addResultReader(InternalReverseChildren::new)
        );
    }


}