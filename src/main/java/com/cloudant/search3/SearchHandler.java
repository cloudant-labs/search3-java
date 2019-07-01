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
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.grouping.GroupingSearch;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.subspace.Subspace;
import com.cloudant.fdblucene.FDBDirectory;
import com.cloudant.search3.grpc.Search3.FieldValue;
import com.cloudant.search3.grpc.Search3.Hit;
import com.cloudant.search3.grpc.Search3.HitField;
import com.cloudant.search3.grpc.Search3.SearchResponse;

/**
 * Provides all services for a specific index; should be cached.
 */
public final class SearchHandler implements AutoCloseable {

    private static final Set<String> ID_SET = Collections.singleton("_id");
    private static final int DEFAULT_N = 25;

    @FunctionalInterface
    private interface IOFunction<T, R> {
        R apply(T t) throws IOException;
    }

    private static final double GROUP_CACHING_MB = 4.0;
    private static final int PAGE_SIZE = 10_000;
    private static final int TXN_SIZE = 10 * PAGE_SIZE;

    private final IndexWriter writer;
    private final SearcherManager manager;

    private String pendingUpdateSeq;
    private String updateSeq;

    public static SearchHandler open(final Database db, final Subspace index, final Analyzer analyzer)
            throws IOException {
        final Directory dir = FDBDirectory.open(db, index, PAGE_SIZE, TXN_SIZE);
        final IndexWriterConfig indexWriterConfig = indexWriterConfig(analyzer);
        final IndexWriter writer = new IndexWriter(dir, indexWriterConfig);
        final SearcherManager manager = new SearcherManager(writer, null);
        return new SearchHandler(writer, manager);
    }

    private SearchHandler(final IndexWriter writer, final SearcherManager manager) {
        this.writer = writer;
        this.manager = manager;
    }

    @Override
    public void close() throws IOException {
        this.manager.close();
        this.writer.rollback();
    }

    public SearchResponse search(final Query query, final int n, final Set<String> fieldsToLoad, final boolean staleOk)
            throws IOException {
        return withSearcher(staleOk, searcher -> {
            final TopDocs topDocs = searcher.search(query, correctN(n));
            final SearchResponse.Builder responseBuilder = SearchResponse.newBuilder();
            responseBuilder.setMatches(topDocs.totalHits.value);
            for (int i = 0; i < topDocs.scoreDocs.length; i++) {
                final Document doc = searcher.doc(topDocs.scoreDocs[i].doc, correctFieldsToLoad(fieldsToLoad));
                final Hit.Builder hitBuilder = Hit.newBuilder();
                hitBuilder.setId(doc.get("_id"));
                hitBuilder.addOrder(FieldValue.newBuilder().setDoubleValue(topDocs.scoreDocs[i].score));
                addFieldsToHit(hitBuilder, doc);
                responseBuilder.addHits(hitBuilder);
            }
            return responseBuilder.build();
        });
    }

    public SearchResponse search(
            final Query query,
            final int n,
            final Sort sort,
            final Set<String> fieldsToLoad,
            final boolean staleOk) throws IOException {
        return withSearcher(staleOk, searcher -> {
            final TopFieldDocs topFieldDocs = searcher.search(query, correctN(n), sort);
            final SearchResponse.Builder responseBuilder = SearchResponse.newBuilder();
            responseBuilder.setMatches(topFieldDocs.totalHits.value);
            for (int i = 0; i < topFieldDocs.scoreDocs.length; i++) {
                final FieldDoc fieldDoc = (FieldDoc) topFieldDocs.scoreDocs[i];
                final Document doc = searcher.doc(fieldDoc.doc, correctFieldsToLoad(fieldsToLoad));
                final Hit.Builder hitBuilder = Hit.newBuilder();
                hitBuilder.setId(doc.get("_id"));
                for (int j = 0; j < fieldDoc.fields.length; j++) {
                    final Object field = fieldDoc.fields[j];
                    if (field instanceof String) {
                        hitBuilder.addOrder(FieldValue.newBuilder().setStringValue((String) field));
                    }
                    if (field instanceof Number) {
                        hitBuilder.addOrder(FieldValue.newBuilder().setDoubleValue(((Number) field).doubleValue()));
                    }
                }
                addFieldsToHit(hitBuilder, doc);
                responseBuilder.addHits(hitBuilder);
            }
            return responseBuilder.build();
        });
    }

    public TopGroups<BytesRef> groupingSearch(
            final Query query,
            final String groupBy,
            final Sort groupSort,
            final int groupOffset,
            final int groupLimit,
            final int groupDocsLimit,
            final boolean staleOk) throws IOException {

        final GroupingSearch groupingSearch = new GroupingSearch(groupBy);
        groupingSearch.setGroupSort(groupSort);
        groupingSearch.setCachingInMB(GROUP_CACHING_MB, true);
        groupingSearch.setAllGroups(false);
        groupingSearch.setGroupDocsLimit(groupDocsLimit);

        return withSearcher(staleOk, searcher -> {
            return groupingSearch.search(searcher, query, groupOffset, groupLimit);
        });
    }

    public void updateDocument(final Term term, final Document doc) throws IOException {
        this.writer.updateDocument(term, doc);
    }

    public void deleteDocuments(final Term... terms) throws IOException {
        this.writer.deleteDocuments(terms);
    }

    public String getUpdateSeq() {
        return updateSeq;
    }

    public void setPendingUpdateSeq(final String pendingUpdateSeq) {
        this.pendingUpdateSeq = pendingUpdateSeq;
    }

    public void commit() throws IOException {
        if (pendingUpdateSeq == null) {
            throw new IllegalStateException("Cannot commit without a new update sequence");
        }
        final Map<String, String> commitData = Collections.singletonMap("update_seq", pendingUpdateSeq);
        this.writer.setLiveCommitData(commitData.entrySet());
        this.writer.commit();
        this.updateSeq = this.pendingUpdateSeq;
        this.pendingUpdateSeq = null;
    }

    private static IndexWriterConfig indexWriterConfig(final Analyzer analyzer) {
        final IndexWriterConfig result = new IndexWriterConfig(analyzer);
        result.setIndexSort(new Sort(new SortField("_id", Type.STRING)));
        result.setUseCompoundFile(false);
        return result;
    }

    private <T> T withSearcher(final boolean staleOk, final IOFunction<IndexSearcher, T> f) throws IOException {
        if (!staleOk) {
            manager.maybeRefreshBlocking();
        }
        final IndexSearcher searcher = manager.acquire();
        try {
            return f.apply(searcher);
        } finally {
            manager.release(searcher);
        }
    }

    private void addFieldsToHit(final Hit.Builder hitBuilder, final Document doc) {
        for (final IndexableField field : doc) {
            final HitField.Builder hitFieldBuilder = HitField.newBuilder();
            hitFieldBuilder.setName(field.name());
            final FieldValue.Builder fieldValueBuilder;
            if (field.stringValue() != null) {
                fieldValueBuilder = FieldValue.newBuilder().setStringValue(field.stringValue());
            } else if (field.numericValue() != null) {
                fieldValueBuilder = FieldValue.newBuilder().setDoubleValue(field.numericValue().doubleValue());
            } else {
                continue;
            }
            hitFieldBuilder.setValue(fieldValueBuilder);
            hitBuilder.addFields(hitFieldBuilder);
        }
    }

    private int correctN(final int n) {
        return n == 0 ? DEFAULT_N : n;
    }

    private Set<String> correctFieldsToLoad(final Set<String> fieldsToLoad) {
        if (fieldsToLoad == null) {
            return ID_SET;
        }
        fieldsToLoad.add("_id");
        return fieldsToLoad;
    }

}
