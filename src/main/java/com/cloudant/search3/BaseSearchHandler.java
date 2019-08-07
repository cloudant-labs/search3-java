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

package com.cloudant.search3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;
import org.apache.lucene.util.BytesRef;

import com.cloudant.search3.grpc.Search3;
import com.cloudant.search3.grpc.Search3.Bookmark;
import com.cloudant.search3.grpc.Search3.DocumentField;
import com.cloudant.search3.grpc.Search3.DocumentUpdateRequest;
import com.cloudant.search3.grpc.Search3.FieldValue;
import com.cloudant.search3.grpc.Search3.GroupSearchRequest;
import com.cloudant.search3.grpc.Search3.Hit;
import com.cloudant.search3.grpc.Search3.HitField;
import com.cloudant.search3.grpc.Search3.Index;
import com.cloudant.search3.grpc.Search3.SearchRequest;
import com.cloudant.search3.grpc.Search3.SearchResponse;
import com.cloudant.search3.grpc.Search3.SessionResponse;
import com.cloudant.search3.grpc.Search3.UpdateSeq;

public abstract class BaseSearchHandler implements SearchHandler {

    private static final FieldValue EMPTY_VALUE = FieldValue.newBuilder().build();
    private static final ScoreDoc[] EMPTY_SCORE_DOC = new ScoreDoc[0];
    private static final SortField INVERSE_FIELD_SCORE = new SortField(null, SortField.Type.SCORE, true);
    private static final SortField INVERSE_FIELD_DOC = new SortField(null, SortField.Type.DOC, true);

    protected static final Pattern SORT_FIELD_RE = Pattern.compile("^([-+])?([\\.\\w]+)(?:<(\\w+)>)?$");
    private static final String FP = "([-+]?[0-9]+(?:\\.[0-9]+)?)";
    private static final Pattern DISTANCE_RE = Pattern
            .compile("^([-+])?<distance,([\\.\\w]+),([\\.\\w]+),%s,%s,(mi|km)>$".format(FP, FP));

    private static final Logger LOGGER = LogManager.getLogger();

    private static final Set<String> ID_SET = Collections.singleton("_id");

    private static Set<String> defaultFieldsToLoad(final Set<String> fieldsToLoad) {
        if (fieldsToLoad == null) {
            return ID_SET;
        }
        fieldsToLoad.add("_id");
        return fieldsToLoad;
    }

    protected Logger logger;
    private final ThreadLocal<QueryParser> queryParser;

    protected BaseSearchHandler(final Analyzer analyzer) {
        this.queryParser = new ThreadLocal<QueryParser>() {

            @Override
            protected QueryParser initialValue() {
                return new Search3QueryParser(analyzer);
            }

        };
    }

    protected final Query parse(final String queryString, final String partition) throws ParseException {
        final Query baseQuery = queryParser.get().parse(queryString);
        if (partition.isEmpty()) {
            return baseQuery;
        } else {
            final BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(new TermQuery(new Term("_partition", partition)), Occur.MUST);
            builder.add(baseQuery, Occur.MUST);
            return builder.build();
        }
    }

    @Override
    public final SearchResponse search(final SearchRequest request) throws IOException, ParseException {
        verifySession(request.getIndex());
        final int limit = request.getLimit();
        final Set<String> fieldsToLoad = toFieldSet(request);
        final boolean staleOk = request.getStale();
        final Sort sort = toSort(request);
        final ScoreDoc after = toAfter(request, sort);
        final Query query = parse(request.getQuery(), request.getPartition());

        return withSearcher(staleOk, searcher -> {
            final TopDocs topDocs;

            if (limit == 0) {
                final CollectorManager<TotalHitCountCollector, TopDocs> manager = totalHitCollector();
                topDocs = searcher.search(query, manager);
            } else if (sort == null) {
                final CollectorManager<TopScoreDocCollector, TopDocs> manager = topScoreDocCollector(
                        searcher,
                        after,
                        limit);
                topDocs = searcher.search(query, manager);
            } else {
                final CollectorManager<TopFieldCollector, TopFieldDocs> manager = topFieldDocCollector(
                        searcher,
                        (FieldDoc) after,
                        sort,
                        limit);
                try {
                    topDocs = searcher.search(query, manager);
                } catch (final IllegalStateException e) {
                    final String message = e.getMessage();
                    if (message != null && message.contains("(expected=NUMERIC)")) {
                        throw new IllegalStateException("cannot sort string field as numeric field", e);
                    }
                    if (message != null && message.contains("(expected=SORTED)")) {
                        throw new IllegalStateException("cannot sort numeric field as string field", e);
                    }
                    throw e;
                }
            }
            final SearchResponse.Builder responseBuilder = SearchResponse.newBuilder();
            responseBuilder.setSession(getSession());
            responseBuilder.setMatches(topDocs.totalHits.value);
            addBookmark(responseBuilder, topDocs);
            for (int i = 0; i < topDocs.scoreDocs.length; i++) {
                final ScoreDoc scoreDoc = topDocs.scoreDocs[i];
                final Document doc;
                if (fieldsToLoad.isEmpty()) {
                    doc = searcher.doc(scoreDoc.doc);
                } else {
                    doc = searcher.doc(scoreDoc.doc, defaultFieldsToLoad(fieldsToLoad));
                }
                final Hit.Builder hitBuilder = Hit.newBuilder();
                hitBuilder.setId(doc.get("_id"));
                addOrderToHit(hitBuilder, scoreDoc);
                addFieldsToHit(hitBuilder, doc);
                responseBuilder.addHits(hitBuilder);
            }
            return responseBuilder.build();
        });
    }

    private void addBookmark(final SearchResponse.Builder responseBuilder, final TopDocs topDocs) {
        if (topDocs.scoreDocs.length > 0) {
            final ScoreDoc lastDoc = topDocs.scoreDocs[topDocs.scoreDocs.length - 1];
            final Bookmark bookmark = Bookmark.newBuilder().addAllOrder(toFieldValues(lastDoc)).build();
            responseBuilder.setBookmark(bookmark);
        }
    }

    protected final void addFieldsToHit(final Hit.Builder hitBuilder, final Document doc) {
        for (final IndexableField field : doc) {
            if ("_id".equals(field.name())) {
                continue;
            }
            final HitField.Builder hitFieldBuilder = HitField.newBuilder();
            hitFieldBuilder.setName(field.name());
            final FieldValue.Builder fieldValueBuilder;
            if (field.stringValue() != null) {
                fieldValueBuilder = FieldValue.newBuilder().setString(field.stringValue());
            } else if (field.numericValue() != null) {
                fieldValueBuilder = FieldValue.newBuilder().setDouble(field.numericValue().doubleValue());
            } else {
                continue;
            }
            hitFieldBuilder.setValue(fieldValueBuilder);
            hitBuilder.addFields(hitFieldBuilder);
        }
    }

    protected final void addOrderToHit(final Hit.Builder hitBuilder, final ScoreDoc scoreDoc) {
        hitBuilder.addAllOrder(toFieldValues(scoreDoc));
    }

    private Iterable<FieldValue> toFieldValues(final ScoreDoc scoreDoc) {
        final List<FieldValue> result = new ArrayList<FieldValue>();
        if (scoreDoc instanceof FieldDoc) {
            final Object[] fields = ((FieldDoc) scoreDoc).fields;
            for (int i = 0; i < fields.length; i++) {
                Object field = fields[i];
                if (field instanceof Number) {
                    final double value = ((Number) field).doubleValue();
                    result.add(FieldValue.newBuilder().setDouble(value).build());
                } else if (field instanceof BytesRef) {
                    final String value = ((BytesRef) field).utf8ToString();
                    result.add(FieldValue.newBuilder().setString(value).build());
                } else if (field == null) {
                    result.add(EMPTY_VALUE);
                } else {
                    throw new IllegalArgumentException("Unknown order value: " + field);
                }
            }
        } else {
            result.add(FieldValue.newBuilder().setFloat(scoreDoc.score).build());
        }
        result.add(FieldValue.newBuilder().setInt(scoreDoc.doc).build());
        return result;
    }

    protected abstract <T> T withSearcher(final boolean staleOk, final IOFunction<IndexSearcher, T> f)
            throws IOException;

    protected abstract String getSession();

    protected final SessionResponse sessionResponse() {
        return SessionResponse.newBuilder().setSession(getSession()).build();
    }

    protected static Set<String> toFieldSet(final SearchRequest request) {
        return new HashSet<String>(request.getIncludeFieldsList());
    }

    protected static Sort toSort(final SearchRequest request) throws ParseException {
        return toSort(request.getSort());
    }

    protected static Sort toSort(final GroupSearchRequest request) throws ParseException {
        return toSort(request.getGroupSort());
    }

    protected static Sort toSort(final Search3.Sort sort) throws ParseException {
        if (sort.getFieldsCount() == 0) {
            return null;
        }

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

    protected static ScoreDoc toAfter(final SearchRequest request, final Sort sort) {
        if (!request.hasBookmark()) {
            return null;
        }

        final Bookmark bookmark = request.getBookmark();

        // Default sort order (by relevance).
        if (sort == null) {
            final float score = bookmark.getOrder(0).getFloat();
            final int doc = bookmark.getOrder(1).getInt();
            return new ScoreDoc(doc, score);
        }

        // Custom sort order.
        final Object[] fields = new Object[bookmark.getOrderCount() - 1];
        for (int i = 0; i < fields.length; i++) {
            final FieldValue value = bookmark.getOrder(i);
            switch (value.getValueCase()) {
            case BOOL:
                fields[i] = value.getBool();
                break;
            case DOUBLE:
                fields[i] = value.getDouble();
                break;
            case FLOAT:
                fields[i] = value.getFloat();
                break;
            case INT:
                fields[i] = value.getInt();
                break;
            case LONG:
                fields[i] = value.getLong();
                break;
            case STRING:
                fields[i] = new BytesRef(value.getString());
                break;
            default:
                throw new IllegalArgumentException(value + " is malformed in bookmark");
            }
        }
        final int doc = bookmark.getOrder(bookmark.getOrderCount() - 1).getInt();
        return new FieldDoc(doc, Float.NaN, fields);
    }

    protected static Document toDoc(final DocumentUpdateRequest request) throws IOException {
        final DocumentBuilder builder = new DocumentBuilder();
        builder.addString("_id", request.getId(), true);

        if (!request.getPartition().isEmpty()) {
            builder.addString("_partition", request.getPartition(), false);
        }

        for (final DocumentField field : request.getFieldsList()) {
            final String name = field.getName();
            final FieldValue value = field.getValue();
            final boolean analyzed = field.getAnalyzed();
            final boolean store = field.getStore();
            final boolean facet = field.getFacet();

            switch (value.getValueCase()) {
            case BOOL:
                builder.addBoolean(name, value.getBool(), store);
                break;
            case INT:
                builder.addLong(name, value.getInt(), store);
                break;
            case LONG:
                builder.addLong(name, value.getLong(), store);
                break;
            case FLOAT:
                builder.addDouble(name, value.getFloat(), store);
                break;
            case DOUBLE:
                builder.addDouble(name, value.getDouble(), store);
                break;
            case STRING:
                if (analyzed) {
                    builder.addText(name, value.getString(), store, facet);
                } else {
                    builder.addString(name, value.getString(), store);
                }
                break;
            default:
                throw new IllegalArgumentException(name + " has no value.");
            }
        }
        return builder.build();
    }

    protected static UpdateSeq seq(final String value) {
        return UpdateSeq.newBuilder().setSeq(value).build();
    }

    protected final void verifySession(final Index index) {
        final String session = index.getSession();
        if (session.isEmpty()) {
            return;
        }
        if (getSession().equals(session)) {
            return;
        }
        throw new SessionMismatchException("session mismatch");
    }

    private CollectorManager<TotalHitCountCollector, TopDocs> totalHitCollector() {
        return new CollectorManager<TotalHitCountCollector, TopDocs>() {

            @Override
            public TotalHitCountCollector newCollector() throws IOException {
                return new TotalHitCountCollector();
            }

            @Override
            public TopDocs reduce(Collection<TotalHitCountCollector> collectors) throws IOException {
                int count = 0;
                for (TotalHitCountCollector collector : collectors) {
                    count += collector.getTotalHits();
                }
                return new TopDocs(new TotalHits(count, Relation.EQUAL_TO), EMPTY_SCORE_DOC);
            }
        };
    }

    private CollectorManager<TopScoreDocCollector, TopDocs> topScoreDocCollector(
            final IndexSearcher searcher,
            final ScoreDoc after,
            final int numHits) {
        final int limit = Math.max(1, searcher.getIndexReader().maxDoc());
        if (after != null && after.doc >= limit) {
            throw new IllegalArgumentException("after.doc exceeds the number of documents in the reader: after.doc="
                    + after.doc + " limit=" + limit);
        }

        final int cappedNumHits = Math.min(numHits, limit);

        return new CollectorManager<TopScoreDocCollector, TopDocs>() {

            @Override
            public TopScoreDocCollector newCollector() throws IOException {
                return TopScoreDocCollector.create(cappedNumHits, after, Integer.MAX_VALUE);
            }

            @Override
            public TopDocs reduce(Collection<TopScoreDocCollector> collectors) throws IOException {
                final TopDocs[] topDocs = new TopDocs[collectors.size()];
                int i = 0;
                for (TopScoreDocCollector collector : collectors) {
                    topDocs[i++] = collector.topDocs();
                }
                return TopDocs.merge(0, cappedNumHits, topDocs, true);
            }

        };
    }

    private CollectorManager<TopFieldCollector, TopFieldDocs> topFieldDocCollector(
            final IndexSearcher searcher,
            final FieldDoc after,
            final Sort sort,
            final int numHits) throws IOException {
        final int limit = Math.max(1, searcher.getIndexReader().maxDoc());
        if (after != null && after.doc >= limit) {
            throw new IllegalArgumentException("after.doc exceeds the number of documents in the reader: after.doc="
                    + after.doc + " limit=" + limit);
        }
        final int cappedNumHits = Math.min(numHits, limit);
        final Sort rewrittenSort = sort.rewrite(searcher);
        return new CollectorManager<TopFieldCollector, TopFieldDocs>() {

            @Override
            public TopFieldCollector newCollector() throws IOException {
                return TopFieldCollector.create(rewrittenSort, cappedNumHits, after, Integer.MAX_VALUE);
            }

            @Override
            public TopFieldDocs reduce(Collection<TopFieldCollector> collectors) throws IOException {
                final TopFieldDocs[] topDocs = new TopFieldDocs[collectors.size()];
                int i = 0;
                for (TopFieldCollector collector : collectors) {
                    topDocs[i++] = collector.topDocs();
                }
                return TopDocs.merge(rewrittenSort, 0, cappedNumHits, topDocs, true);
            }

        };
    }

}
