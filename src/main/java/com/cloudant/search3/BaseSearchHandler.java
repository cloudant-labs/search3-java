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
import java.util.Collections;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;

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

    private static int defaultN(final int n) {
        return n == 0 ? DEFAULT_N : n;
    }

    protected Logger logger;

    @Override
    public final SearchResponse search(
            final Query query,
            final int n,
            final Set<String> fieldsToLoad,
            final boolean staleOk) throws IOException {
        logger.info("search {} {}", query, n);
        return withSearcher(staleOk, searcher -> {
            final TopDocs topDocs = searcher.search(query, defaultN(n));
            final SearchResponse.Builder responseBuilder = SearchResponse.newBuilder();
            responseBuilder.setMatches(topDocs.totalHits.value);
            for (int i = 0; i < topDocs.scoreDocs.length; i++) {
                final Document doc;
                if (fieldsToLoad.isEmpty()) {
                    doc = searcher.doc(topDocs.scoreDocs[i].doc);
                } else {
                    doc = searcher.doc(topDocs.scoreDocs[i].doc, defaultFieldsToLoad(fieldsToLoad));
                }
                final Hit.Builder hitBuilder = Hit.newBuilder();
                hitBuilder.setId(doc.get("_id"));
                addFieldsToHit(hitBuilder, doc);
                responseBuilder.addHits(hitBuilder);
            }
            return responseBuilder.build();
        });
    }

    @Override
    public final SearchResponse search(
            final Query query,
            final int n,
            final Sort sort,
            final Set<String> fieldsToLoad,
            final boolean staleOk) throws IOException {
        logger.info("search {} {} {}", query, n, sort);
        return withSearcher(staleOk, searcher -> {
            final TopFieldDocs topFieldDocs = searcher.search(query, defaultN(n), sort);
            final SearchResponse.Builder responseBuilder = SearchResponse.newBuilder();
            responseBuilder.setMatches(topFieldDocs.totalHits.value);
            for (int i = 0; i < topFieldDocs.scoreDocs.length; i++) {
                final FieldDoc fieldDoc = (FieldDoc) topFieldDocs.scoreDocs[i];
                final Document doc = searcher.doc(fieldDoc.doc, defaultFieldsToLoad(fieldsToLoad));
                final Hit.Builder hitBuilder = Hit.newBuilder();
                hitBuilder.setId(doc.get("_id"));
                addFieldsToHit(hitBuilder, doc);
                responseBuilder.addHits(hitBuilder);
            }
            return responseBuilder.build();
        });
    }

    private void addFieldsToHit(final Hit.Builder hitBuilder, final Document doc) {
        for (final IndexableField field : doc) {
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

    protected abstract <T> T withSearcher(final boolean staleOk, final IOFunction<IndexSearcher, T> f)
            throws IOException;
}
