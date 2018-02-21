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

import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.plain.SortedSetDVOrdinalsIndexFieldData;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.ParentFieldMapper;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.FieldContext;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource.Bytes.WithOrdinals;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.internal.SearchContext;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Objects;

public class ReverseChildrenAggregationBuilder
    extends ValuesSourceAggregationBuilder<WithOrdinals, ReverseChildrenAggregationBuilder> {

    public static final String NAME = "reverse_children";

    private final String childType;
    private Query parentFilter;
    private Query childFilter;

    /**
     * @param name
     *            the name of this aggregation
     * @param childType
     *            the type of children documents
     */
    public ReverseChildrenAggregationBuilder(String name, String childType) {
        super(name, ValuesSourceType.BYTES, ValueType.STRING);
        if (childType == null) {
            throw new IllegalArgumentException("[childType] must not be null: [" + name + "]");
        }
        this.childType = childType;
    }

    /**
     * Read from a stream.
     */
    public ReverseChildrenAggregationBuilder(StreamInput in) throws IOException {
        super(in, ValuesSourceType.BYTES, ValueType.STRING);
        childType = in.readString();
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeString(childType);
    }

    @Override
    protected ValuesSourceAggregatorFactory<WithOrdinals, ?> innerBuild(SearchContext context,
                                                                        ValuesSourceConfig<WithOrdinals> config,
                                                                        AggregatorFactory<?> parent,
                                                                        Builder subFactoriesBuilder) throws IOException {
        return new ReverseChildrenAggregatorFactory(name, config, childFilter, parentFilter, context, parent,
            subFactoriesBuilder, metaData);
    }

    @Override
    protected ValuesSourceConfig<WithOrdinals> resolveConfig(SearchContext context) {
        ValuesSourceConfig<WithOrdinals> config = new ValuesSourceConfig<>(ValuesSourceType.BYTES);
        if (context.mapperService().getIndexSettings().isSingleType()) {
            joinFieldResolveConfig(context, config);
        } else {
            parentFieldResolveConfig(context, config);
        }

        //ValuesSourceConfig.resolve(context, ValueType.STRING, "_id", null, null, null, null);

        return config;
    }


    private void joinFieldResolveConfig(SearchContext context, ValuesSourceConfig<WithOrdinals> config) {
        // get Mapper by reflection because mappers are in a module classloader
        /*
        ParentJoinFieldMapper parentJoinFieldMapper = ParentJoinFieldMapper.getMapper(context.mapperService());
        ParentIdFieldMapper parentIdFieldMapper = parentJoinFieldMapper.getParentIdFieldMapper(childType, false);
        if (parentIdFieldMapper != null) {
            parentFilter = parentIdFieldMapper.getParentFilter();
            childFilter = parentIdFieldMapper.getChildFilter(childType);
            MappedFieldType fieldType = parentIdFieldMapper.fieldType();
            final SortedSetDVOrdinalsIndexFieldData fieldData = context.getForField(fieldType);
            config.fieldContext(new FieldContext(fieldType.name(), fieldData, fieldType));
        } else {
            config.unmapped(true);
        }
        */
        MappedFieldType fieldType = context.mapperService().fullName("_parent_join");

        if (fieldType != null) {
            try {
                Method getMapper = fieldType.getClass().getMethod("getMapper", (Class[])null);
                FieldMapper parentJoinFieldMapper  = (FieldMapper)getMapper.invoke(fieldType, (Object[])null);
                Method getParentIdFieldMapper = parentJoinFieldMapper.getClass().getMethod("getParentIdFieldMapper",
                        String.class,
                        boolean.class);
                FieldMapper parentIdFieldMapper = (FieldMapper)getParentIdFieldMapper.invoke(parentJoinFieldMapper,
                        childType,
                        false);
                if (parentIdFieldMapper != null) {
                    Method getParentFilter = parentIdFieldMapper.getClass().getMethod("getParentFilter", (Class[])null);
                    parentFilter = (Query)getParentFilter.invoke(parentIdFieldMapper, (Object[])null);
                    Method getChildFilter = parentIdFieldMapper.getClass().getMethod("getChildFilter", String.class);
                    childFilter = (Query)getChildFilter.invoke(parentIdFieldMapper, childType);
                    parentFilter = (Query)getParentFilter.invoke(parentIdFieldMapper, (Object[])null);

                    Method fieldTypeMethod = parentIdFieldMapper.getClass().getMethod("fieldType", (Class[])null);
                    MappedFieldType fieldType2 = (MappedFieldType)fieldTypeMethod.invoke(parentIdFieldMapper, (Object[])null);

                    final SortedSetDVOrdinalsIndexFieldData fieldData = context.getForField(fieldType2);
                    config.fieldContext(new FieldContext(fieldType2.name(), fieldData, fieldType2));
                }
                else {
                    config.unmapped(true);
                }

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }


    }

    private void parentFieldResolveConfig(SearchContext context, ValuesSourceConfig<WithOrdinals> config) {
        DocumentMapper childDocMapper = context.mapperService().documentMapper(childType);
        if (childDocMapper != null) {
            ParentFieldMapper parentFieldMapper = childDocMapper.parentFieldMapper();
            if (!parentFieldMapper.active()) {
                throw new IllegalArgumentException("[children] no [_parent] field not configured that points to a parent type");
            }
            String parentType = parentFieldMapper.type();
            DocumentMapper parentDocMapper = context.mapperService().documentMapper(parentType);
            if (parentDocMapper != null) {
                parentFilter = parentDocMapper.typeFilter(context.getQueryShardContext());
                childFilter = childDocMapper.typeFilter(context.getQueryShardContext());
                MappedFieldType parentFieldType = parentDocMapper.parentFieldMapper().getParentJoinFieldType();
                final SortedSetDVOrdinalsIndexFieldData fieldData = context.getForField(parentFieldType);
                config.fieldContext(new FieldContext(parentFieldType.name(), fieldData,
                    parentFieldType));
            } else {
                config.unmapped(true);
            }
        } else {
            config.unmapped(true);
        }


    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(ReverseChildrenToParentAggregator.TYPE_FIELD.getPreferredName(), childType);
        return builder;
    }

    public static ReverseChildrenAggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        String childType = null;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("type".equals(currentFieldName)) {
                    childType = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                        "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "Unexpected token " + token + " in [" + aggregationName + "].");
            }
        }

        if (childType == null) {
            throw new ParsingException(parser.getTokenLocation(),
                "Missing [child_type] field for children aggregation [" + aggregationName + "]");
        }

        return new ReverseChildrenAggregationBuilder(aggregationName, childType);
    }

    @Override
    protected int innerHashCode() {
        return Objects.hash(childType);
    }

    @Override
    protected boolean innerEquals(Object obj) {
        ReverseChildrenAggregationBuilder other = (ReverseChildrenAggregationBuilder) obj;
        return Objects.equals(childType, other.childType);
    }

    @Override
    public String getType() {
        return NAME;
    }
}
