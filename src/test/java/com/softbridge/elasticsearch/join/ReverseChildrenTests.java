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

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.join.ParentJoinPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.nestedQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.join.query.JoinQueryBuilders.hasChildQuery;
import static org.elasticsearch.join.query.JoinQueryBuilders.hasParentQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasId;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ReverseChildrenTests extends ESIntegTestCase {
    /**
     * Returns a collection of plugins that should be loaded on each node.
     */
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ReverseChildrenAggPlugin.class, ParentJoinPlugin.class);
    }

    public void testPlugin() throws IOException, ExecutionException, InterruptedException {

        XContentBuilder builder = jsonBuilder().startObject().startObject("case").startObject("properties")
                .startObject("join_field")
                .field("type", "join")
                .startObject("relations")
                .field("case", "activity")
                .endObject()
                .endObject()
                .startObject("type")
                .field("type", "keyword")
                .endObject()
                .startObject("action")
                .field("type", "text")
                .field("fielddata", true)
                .endObject()
                .startObject("actionId")
                .field("type", "long")
                .endObject()
                .endObject().endObject().endObject();
        String json = builder.string();
        assertAcked(prepareCreate("cases")
                .addMapping("case", builder
                ));


        List<IndexRequestBuilder> requests = new ArrayList<>();
        requests.add(createIndexRequest("cases", "case", "case1", null, "type", "case", "action", "actionA", "actionId", 1));
        requests.add(createIndexRequest("cases", "case", "case2", null, "type", "case", "action", "actionB", "actionId", 2));

        requests.add(createIndexRequest("cases", "activity", "activity1", "case1", "type", "activity", "action", "actionA", "actionId", 3));
        requests.add(createIndexRequest("cases", "activity", "activity2", "case1", "type", "activity", "action", "actionB", "actionId", 4));
        requests.add(createIndexRequest("cases", "activity", "activity3", "case1", "type", "activity", "action", "actionC", "actionId", 5));
        requests.add(createIndexRequest("cases", "activity", "activity4", "case1", "type", "activity", "action", "actionC", "actionId", 5));

        requests.add(createIndexRequest("cases", "activity", "activity5", "case2", "type", "activity", "action", "actionB", "actionId", 4));
        requests.add(createIndexRequest("cases", "activity", "activity6", "case2", "type", "activity", "action", "actionC", "actionId", 5));
        requests.add(createIndexRequest("cases", "activity", "activity7", "case2", "type", "activity", "action", "actionD", "actionId", 6));

        indexRandom(true, requests);

        SearchRequestBuilder requestBuilder = client().prepareSearch("*").setQuery(QueryBuilders.termQuery("type", "activity"))
               .addAggregation(AggregationBuilders.terms("actions").field("action").subAggregation(ReverseJoinAggregationBuilders.reverseChildren("cases", "activity")));
        SearchResponse searchResponse = requestBuilder.execute().actionGet();
        SearchHits hits = searchResponse.getHits();
        assertEquals(7, hits.totalHits);

        StringTerms agg = searchResponse.getAggregations().get("actions");
        assertEquals(4, agg.getBuckets().size());

        assertEquals(1, agg.getBucketByKey("actiona").getDocCount());
        ReverseChildren reverseChildren = agg.getBucketByKey("actiona").getAggregations().get("cases");
        assertEquals(1, reverseChildren.getDocCount());

        assertEquals(2, agg.getBucketByKey("actionb").getDocCount());
        reverseChildren = agg.getBucketByKey("actionb").getAggregations().get("cases");
        assertEquals(2, reverseChildren.getDocCount());

        assertEquals(3, agg.getBucketByKey("actionc").getDocCount());
        reverseChildren = agg.getBucketByKey("actionc").getAggregations().get("cases");
        assertEquals(2, reverseChildren.getDocCount());

        assertEquals(1, agg.getBucketByKey("actiond").getDocCount());
        reverseChildren = agg.getBucketByKey("actiond").getAggregations().get("cases");
        assertEquals(1, reverseChildren.getDocCount());

    }


    protected IndexRequestBuilder createIndexRequest(String index, String type, String id, String parentId, Object... fields) {
        Map<String, Object> source = new HashMap<>();
        for (int i = 0; i < fields.length; i += 2) {
            source.put((String) fields[i], fields[i + 1]);
        }
        return createIndexRequest(index, type, id, parentId, source);
    }

    private IndexRequestBuilder createIndexRequest(String index, String type, String id, String parentId, Map<String, Object> source) {
        String name = type;

        IndexRequestBuilder indexRequestBuilder = client().prepareIndex(index, "case", id);
        Map<String, Object> joinField = new HashMap<>();
            if (parentId != null) {
                joinField.put("name", name);
                joinField.put("parent", parentId);
                indexRequestBuilder.setRouting(parentId);
            } else {
                joinField.put("name", name);
            }
            source.put("join_field", joinField);
            indexRequestBuilder.setSource(source);

        return indexRequestBuilder;
    }
}
