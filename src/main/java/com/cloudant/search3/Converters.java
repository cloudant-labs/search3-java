// Copyright 2019 IBM
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This class has static methods for converting between our GRPC messages
// and Lucene's domain objects.

package com.cloudant.search3;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;

import com.cloudant.search3.grpc.Search3;
import com.cloudant.search3.grpc.Search3.DocumentField;
import com.cloudant.search3.grpc.Search3.DocumentUpdate;
import com.cloudant.search3.grpc.Search3.FieldValue;
import com.cloudant.search3.grpc.Search3.GroupSearchRequest;
import com.cloudant.search3.grpc.Search3.SearchRequest;

public final class Converters {

    private static final Analyzer analyzer = new StandardAnalyzer(); // TODO specify by index.
    private static final QueryParser queryParser = new QueryParser("default", analyzer);

    private static final SortField INVERSE_FIELD_SCORE = new SortField(null, SortField.Type.SCORE, true);
    private static final SortField INVERSE_FIELD_DOC = new SortField(null, SortField.Type.DOC, true);

    private static final Pattern SORT_FIELD_RE = Pattern.compile("^([-+])?([\\.\\w]+)(?:<(\\w+)>)?$");
    private static final String FP = "([-+]?[0-9]+(?:\\.[0-9]+)?)";
    private static final Pattern DISTANCE_RE = Pattern
            .compile("^([-+])?<distance,([\\.\\w]+),([\\.\\w]+),%s,%s,(mi|km)>$".format(FP, FP));

    private Converters() {
    }

    public static Set<String> toFieldSet(final SearchRequest request) {
        return new HashSet<String>(request.getIncludeFieldsList());
    }

    public static Sort toSort(final SearchRequest request) throws ParseException {
        return toSort(request.getSort());
    }

    public static Sort toSort(final GroupSearchRequest request) throws ParseException {
        return toSort(request.getGroupSort());
    }

    public static Sort toSort(final Search3.Sort sort) throws ParseException {
        final SortField[] sortFields = new SortField[sort.getFieldsCount()];
        for (int i = 0; i < sort.getFieldsCount(); i++) {
            switch (sort.getFields(i)) {
            case "<score>":
                sortFields[i] = INVERSE_FIELD_SCORE;
                continue;
            case "-<score>":
                sortFields[i] = SortField.FIELD_SCORE;
                continue;
            case "<doc>":
                sortFields[i] = SortField.FIELD_DOC;
                continue;
            case "-<doc>":
                sortFields[i] = INVERSE_FIELD_DOC;
                continue;
            }

            Matcher m = DISTANCE_RE.matcher(sort.getFields(i));
            if (m.matches()) {
                throw new ParseException("sort by distance not yet supported.");
            }

            m = SORT_FIELD_RE.matcher(sort.getFields(i));
            if (m.matches()) {
                final String fieldTypeStr = m.group(3) == null ? "number" : m.group(3);
                final SortField.Type fieldType;
                switch (fieldTypeStr) {
                case "string":
                    fieldType = SortField.Type.STRING;
                    break;
                case "number":
                    fieldType = SortField.Type.DOUBLE;
                    break;
                default:
                    throw new ParseException("Unrecognized type: " + m.group(3));
                }
                final boolean reverse = "-".equals(m.group(1));

                sortFields[i] = new SortField(m.group(2), fieldType, reverse);
            }
        }
        return new Sort(sortFields);
    }

    public static Document toDoc(final DocumentUpdate request) throws IOException {
        final DocumentBuilder builder = new DocumentBuilder();
        builder.addString("_id", request.getId(), true);

        for (final DocumentField field : request.getFieldsList()) {
            final String name = field.getName();
            final FieldValue value = field.getValue();
            final boolean analyzed = field.getAnalyzed();
            final boolean stored = field.getStored();
            final boolean facet = field.getFacet();

            switch (value.getValueCase()) {
            case BOOL:
                builder.addBoolean(name, value.getBool(), stored);
                break;
            case DOUBLE:
                builder.addDouble(name, value.getDouble(), stored);
                break;
            case STRING:
                if (analyzed) {
                    builder.addText(name, value.getString(), stored, facet);
                } else {
                    builder.addString(name, value.getString(), stored);
                }
                break;
            default:
                throw new IOException(name + " has no value.");
            }
        }
        return builder.build();
    }

    public static Query toQuery(final SearchRequest request) throws ParseException {
        return toQuery(request.getQuery(), request.getPartition());
    }

    public static Query toQuery(final GroupSearchRequest request) throws ParseException {
        return toQuery(request.getQuery(), "");
    }

    /**
     * Does "partition" even make sense for couch-on-fdb?
     */
    private static Query toQuery(final String queryString, final String partition) throws ParseException {
        final Query baseQuery = queryParser.parse(queryString);
        if (partition.length() > 0) {
            final BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(new TermQuery(new Term("_partition", partition)), Occur.MUST);
            builder.add(baseQuery, Occur.MUST);
            return builder.build();
        } else {
            return baseQuery;
        }
    }

}
