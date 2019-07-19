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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.GroupingSearch;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.util.BytesRef;

import com.cloudant.search3.grpc.Search3.Group;
import com.cloudant.search3.grpc.Search3.GroupSearchResponse;
import com.cloudant.search3.grpc.Search3.Hit;
import com.cloudant.search3.grpc.Search3.InfoResponse;
import com.cloudant.search3.grpc.Search3.UpdateSeq;

/**
 * Provides all services for a specific index using an FDBDirectory; should be
 * cached.
 */
public final class FDBDirectorySearchHandler extends BaseSearchHandler {

    private static final double GROUP_CACHING_MB = 4.0;

    private final String toString;
    private final IndexWriter writer;
    private final SearcherManager manager;
    private UpdateSeq pendingUpdateSeq;
    private boolean dirty = false;

    FDBDirectorySearchHandler(final IndexWriter writer, final SearcherManager manager, final Analyzer analyzer) {
        super(analyzer);
        this.toString = String.format("FDBDirectorySearchHandler(%s)", writer.getDirectory());
        this.logger = LogManager.getLogger(writer.getDirectory().toString());
        this.writer = writer;
        this.manager = manager;
    }

    @Override
    public void close() throws IOException {
        this.manager.close();
        this.writer.rollback();
    }

    @Override
    public GroupSearchResponse groupingSearch(
            final Query query,
            final String groupBy,
            final Sort groupSort,
            final int groupOffset,
            final int groupLimit,
            final int groupDocsLimit,
            final boolean staleOk) throws IOException {

        final GroupingSearch groupingSearch = new GroupingSearch(groupBy);
        if (groupSort != null) {
            groupingSearch.setGroupSort(groupSort);
        }
        groupingSearch.setCachingInMB(GROUP_CACHING_MB, true);
        groupingSearch.setAllGroups(false);
        groupingSearch.setGroupDocsLimit(defaultN(groupDocsLimit));

        return withSearcher(staleOk, searcher -> {
            final TopGroups<BytesRef> result = groupingSearch
                    .search(searcher, query, groupOffset, defaultN(groupLimit));
            final GroupSearchResponse.Builder responseBuilder = GroupSearchResponse.newBuilder();
            responseBuilder.setMatches(result.totalHitCount);
            responseBuilder.setGroupMatches(result.totalGroupedHitCount);

            for (final GroupDocs<BytesRef> group : result.groups) {
                final Group.Builder groupBuilder = Group.newBuilder();
                groupBuilder.setMatches(group.totalHits.value);
                groupBuilder.setBy(group.groupValue.utf8ToString());
                for (final ScoreDoc scoreDoc : group.scoreDocs) {
                    final Document doc = searcher.doc(scoreDoc.doc);
                    final Hit.Builder hitBuilder = Hit.newBuilder();
                    hitBuilder.setId(doc.get("_id"));
                    addOrderToHit(hitBuilder, scoreDoc);
                    addFieldsToHit(hitBuilder, doc);
                    groupBuilder.addHits(hitBuilder);
                }
                responseBuilder.addGroups(groupBuilder);
            }

            return responseBuilder.build();
        });

    }

    @Override
    public InfoResponse info() throws IOException {
        return null;
    }

    @Override
    public void updateDocument(final UpdateSeq seq, final Term term, final Document doc) throws IOException {
        this.writer.updateDocument(term, doc);
        this.pendingUpdateSeq = seq;
        this.dirty = true;
    }

    @Override
    public void deleteDocument(final UpdateSeq seq, final Term term) throws IOException {
        this.writer.deleteDocuments(term);
        this.pendingUpdateSeq = seq;
        this.dirty = true;
    }

    @Override
    public String getUpdateSeq() {
        final Map<String, String> commitData = getLiveCommitData();
        if (commitData != null) {
            final String updateSeq = commitData.get("update_seq");
            if (updateSeq != null) {
                return updateSeq;
            }
        }
        return "0";
    }

    @Override
    public void commit() throws IOException {
        final UpdateSeq committingSeq = pendingUpdateSeq;
        if (dirty && committingSeq != null) {
            try {
                this.writer.setLiveCommitData(createLiveCommitData("update_seq", committingSeq.getSeq()));
                this.writer.commit();
                this.pendingUpdateSeq = null;
                this.dirty = false;
                logger.info("committed at update sequence \"{}\".", committingSeq);
            } catch (final IOException e) {
                logger.catching(e);
                throw e;
            }
        }
    }

    private Map<String, String> getLiveCommitData() {
        final Iterable<Entry<String, String>> it = writer.getLiveCommitData();
        if (it == null) {
            return null;
        }
        final Map<String, String> result = new HashMap<String, String>();
        for (final Entry<String, String> entry : it) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    private Iterable<Entry<String, String>> createLiveCommitData(final String key, final String value) {
        return Collections.singletonMap(key, value).entrySet();
    }

    @Override
    protected <T> T withSearcher(final boolean staleOk, final IOFunction<IndexSearcher, T> f) throws IOException {
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

    @Override
    public String toString() {
        return toString;
    }

}
