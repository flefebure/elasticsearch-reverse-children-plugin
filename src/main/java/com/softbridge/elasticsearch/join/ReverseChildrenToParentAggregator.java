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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

// The RecordingPerReaderBucketCollector assumes per segment recording which isn't the case for this
// aggregation, for this reason that collector can't be used
public class ReverseChildrenToParentAggregator extends BucketsAggregator implements SingleBucketAggregator {

    static final ParseField TYPE_FIELD = new ParseField("type");

    private final Weight childFilter;
    private final Weight parentFilter;
    private final ValuesSource.Bytes.WithOrdinals valuesSource;


    private final LongArray childrenOrdToBuckets;

    public ReverseChildrenToParentAggregator(String name, AggregatorFactories factories,
                                             SearchContext context, Aggregator parent, Query childFilter,
                                             Query parentFilter, ValuesSource.Bytes.WithOrdinals valuesSource,
                                             long maxOrd, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
            throws IOException {
        super(name, factories, context, parent, pipelineAggregators, metaData);
        // these two filters are cached in the parser
        this.childFilter = context.searcher().createNormalizedWeight(childFilter, false);
        this.parentFilter = context.searcher().createNormalizedWeight(parentFilter, false);
        this.childrenOrdToBuckets = context.bigArrays().newLongArray(maxOrd, false);
        this.childrenOrdToBuckets.fill(0, maxOrd, -1);
        this.valuesSource = valuesSource;


    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        return new InternalReverseChildren(name, bucketDocCount(owningBucketOrdinal),
                bucketAggregations(owningBucketOrdinal), pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalReverseChildren(name, 0, buildEmptySubAggregations(), pipelineAggregators(),
                metaData());
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final SortedSetDocValues globalOrdinals = valuesSource.globalOrdinalsValues(ctx);

        return new LeafBucketCollector() {

            @Override
            public void collect(int docId, long bucket) throws IOException {
                if (globalOrdinals.advanceExact(docId)) {
                    long globalOrdinal = globalOrdinals.nextOrd();
                    assert globalOrdinals.nextOrd() == SortedSetDocValues.NO_MORE_ORDS;
                    if (globalOrdinal != -1) {
                        if (childrenOrdToBuckets.get(globalOrdinal) == -1) {
                            childrenOrdToBuckets.set(globalOrdinal, bucket);
                        }
                    }
                }
            }
        };
    }

    @Override
    protected void doPostCollection() throws IOException {
        IndexReader indexReader = context().searcher().getIndexReader();
        for (LeafReaderContext ctx : indexReader.leaves()) {
            Scorer parentDocsScorer = parentFilter.scorer(ctx);
            if (parentDocsScorer == null) {
                continue;
            }
            DocIdSetIterator parentDocsIter = parentDocsScorer.iterator();

            final LeafBucketCollector sub = collectableSubAggregators.getLeafCollector(ctx);

            final SortedSetDocValues globalOrdinals = valuesSource.globalOrdinalsValues(ctx);
            // Set the scorer, since we now replay only the child docIds
            sub.setScorer(new ConstantScoreScorer(null, 1f, parentDocsIter));

            final Bits liveDocs = ctx.reader().getLiveDocs();
            for (int docId = parentDocsIter
                    .nextDoc(); docId != DocIdSetIterator.NO_MORE_DOCS; docId = parentDocsIter
                            .nextDoc()) {
                if (liveDocs != null && liveDocs.get(docId) == false) {
                    continue;
                }
                if (globalOrdinals.advanceExact(docId)) {
                    long globalOrdinal = globalOrdinals.nextOrd();
                    assert globalOrdinals.nextOrd() == SortedSetDocValues.NO_MORE_ORDS;
                    long bucketOrd = childrenOrdToBuckets.get(globalOrdinal);
                    if (bucketOrd != -1) {
                        collectBucket(sub, docId, bucketOrd);

                    }
                }
            }
        }
    }

    @Override
    protected void doClose() {
        Releasables.close(childrenOrdToBuckets);
    }
}
