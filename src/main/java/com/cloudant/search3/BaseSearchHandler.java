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
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;

import com.cloudant.search3.grpc.Search3.Bookmark;
import com.cloudant.search3.grpc.Search3.FieldValue;
import com.cloudant.search3.grpc.Search3.Hit;
import com.cloudant.search3.grpc.Search3.HitField;
import com.cloudant.search3.grpc.Search3.SearchResponse;

public abstract class BaseSearchHandler implements SearchHandler {

    private static final Set<String> ID_SET = Collections.singleton("_id");

    private static final int DEFAULT_N = 25;

    private static Set<String> defaultFieldsToLoad(final Set<String> fieldsToLoad) {
        if (fieldsToLoad == null) {
            return ID_SET;
        }
        fieldsToLoad.add("_id");
        return fieldsToLoad;
    }

    protected static int defaultN(final int n) {
        return n == 0 ? DEFAULT_N : n;
    }

    protected Logger logger;

    @Override
    public final SearchResponse search(
            final ScoreDoc after,
            final Query query,
            final int n,
            final Sort sort,
            final Set<String> fieldsToLoad,
            final boolean staleOk) throws IOException {
        return withSearcher(staleOk, searcher -> {
            final TopDocs topDocs;
            if (sort == null) {
                topDocs = searcher.searchAfter(after, query, defaultN(n));
            } else {
                topDocs = searcher.searchAfter(after, query, defaultN(n), sort);
            }
            final SearchResponse.Builder responseBuilder = SearchResponse.newBuilder();
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
                } else {
                    logger.error("Unknown order value {} of type {}", field, field.getClass());
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
}
