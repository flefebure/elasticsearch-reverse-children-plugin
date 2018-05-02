package com.softbridge.elasticsearch.join;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.CompositeReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.AtomicOrdinalsFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.plain.SortedSetDVBytesAtomicFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetDVOrdinalsIndexFieldData;
import org.elasticsearch.index.mapper.AllFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.support.QueryParsers;
import org.elasticsearch.index.search.QueryParserHelper;
import org.elasticsearch.index.search.QueryStringQueryParser;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.FilterScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.lookup.DocLookup;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SoftbridgeScriptEngine extends Plugin implements ScriptEngine {
    @Override
    public String getType() {
        return "softbridge_script";
    }


    @Override
    public <T> T compile(String scriptName, String scriptSource, ScriptContext<T> context, Map<String, String> params) {

        if (!context.equals(FilterScript.CONTEXT)) {
            throw new IllegalArgumentException(getType() + " scripts cannot be used for context [" + context.name + "]");
        }

        FilterScript.Factory factory = (p, lookup) -> new FilterScript.LeafFactory() {


            @Override
            public FilterScript newInstance(LeafReaderContext context) throws IOException {

                System.out.println("ici");
                return new MyFilterScript(p, lookup, context);
            }

        };

        return context.factoryClazz.cast(factory);

    }

    @Override
    public void close() {
        // optionally close resources
    }

    class MyFilterScript extends FilterScript {

        SearchLookup lookup;
        LeafReaderContext leafContext;


        public MyFilterScript(Map<String, Object> params, SearchLookup lookup, LeafReaderContext leafContext) throws IOException {
            super(params, lookup, leafContext);
            this.lookup = lookup;
            this.leafContext = leafContext;

            MapperService mapperService = lookup.doc().mapperService();
            MappedFieldType fieldType = mapperService.fullName("action.raw");
            Map<String, ScriptDocValues<?>>   leafDocLookup = lookup.getLeafSearchLookup(leafContext).doc();
             IndexFieldData<?> actionFieldData = lookup.doc().getForField(fieldType);
            SortedSetDVBytesAtomicFieldData leafData = (SortedSetDVBytesAtomicFieldData)actionFieldData.load(leafContext);
            SortedSetDocValues sortedSetDocValues = leafData.getOrdinalsValues();

            Query parentFilter = new TermQuery(new Term("esJoin", "case"));
            Query childFilter = new TermQuery(new Term("esJoin", "activity"));


            CompositeReader compositeReader = leafContext.parent.reader();
            IndexSearcher searcher = new IndexSearcher(compositeReader);

            Weight childWeight = childFilter.createWeight(searcher, true, 1f);
            Weight filterWeight = parentFilter.createWeight(searcher, true, 1f);

            String childType = "activity";
             fieldType = mapperService.fullName("esJoin#case");

            //SortedSetDVOrdinalsIndexFieldData fieldData = context.getForField(fieldType);
            SortedSetDVOrdinalsIndexFieldData fieldData = (SortedSetDVOrdinalsIndexFieldData)lookup.doc().getForField(fieldType);

            for (LeafReaderContext ctx : compositeReader.leaves()) {
                ctx.reader();
                final IndexOrdinalsFieldData global = fieldData.loadGlobal((DirectoryReader)ctx.parent.reader());
                final AtomicOrdinalsFieldData atomicFieldData = global.load(ctx);
                SortedSetDocValues globalOrdinals = atomicFieldData.getOrdinalsValues();
                Scorer childDocsScorer = childWeight.scorer(ctx);
                if (childDocsScorer == null) {
                    continue;
                }
                DocIdSetIterator childDocsIter = childDocsScorer.iterator();
               // final SortedSetDocValues globalOrdinals = fieldData.globalOrdinalsValues(ctx);
            }
        }



        @Override
        public boolean execute() {
            Map<String, ScriptDocValues<?>> map = getDoc();
            ScriptDocValues<?> scriptDocValues = map.get("esJoin#case");
            String id = (String)scriptDocValues.get(0);
            MapperService mapper = lookup.doc().mapperService();
            MappedFieldType fieldType = mapper.fullName("action.raw");
            System.out.println(map+" : "+id);

            CompositeReader compositeReader = leafContext.parent.reader();
            IndexSearcher searcher = new IndexSearcher(compositeReader);



            Query childFilter = new TermQuery(new Term("esJoin#case", id));
            try {
                Weight childWeight = childFilter.createWeight(searcher, true, 1f);
                Fields fields = MultiFields.getFields(compositeReader);
                Terms terms = fields.terms("action.raw");

                for (LeafReaderContext ctx : compositeReader.leaves()) {
                    Scorer childDocsScorer = childWeight.scorer(ctx);
                    if (childDocsScorer == null) {
                        continue;
                    }
                    DocIdSetIterator childDocsIter = childDocsScorer.iterator();


                    final Bits liveDocs = ctx.reader().getLiveDocs();
                    for (int docId = childDocsIter
                            .nextDoc(); docId != DocIdSetIterator.NO_MORE_DOCS; docId = childDocsIter
                            .nextDoc()) {
                        System.out.println("docId : "+docId);
                        /*
                        SearchLookup childLookup = new SearchLookup(mapper,
                                mappedFieldType -> indexFieldDataService.apply(mappedFieldType, fullyQualifiedIndexName), types);
                        */

                        SortedSetDocValues sortedSetDocValues = ctx.reader().getSortedSetDocValues("action.raw");
                        sortedSetDocValues.advanceExact(docId);
                        long ord = sortedSetDocValues.nextOrd();
                        BytesRef bytesRef = sortedSetDocValues.lookupOrd(ord);
                        String action = bytesRef.utf8ToString();
                        long vc = sortedSetDocValues.getValueCount();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }



            IndexFieldData<?> fieldData = lookup.doc().getForField(fieldType);
            int size = getParams().size();
            DocLookup dl = lookup.doc();
            //LeafSearchLookup leafSearchLookup = (LeafSearchLookup)lookup;

            return true;
        }
    }

    private MultiFieldQueryParser buildQueryParser() {
        MultiFieldQueryParser multiFieldQueryParser = new MultiFieldQueryParser(new String[]{"role", "action"}, new StandardAnalyzer());
        return multiFieldQueryParser;
    }
}
