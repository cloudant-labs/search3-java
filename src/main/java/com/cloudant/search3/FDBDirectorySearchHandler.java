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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.GroupingSearch;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.util.BytesRef;

import com.cloudant.search3.grpc.Search3.DocumentDeleteRequest;
import com.cloudant.search3.grpc.Search3.DocumentUpdateRequest;
import com.cloudant.search3.grpc.Search3.Group;
import com.cloudant.search3.grpc.Search3.GroupSearchRequest;
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
    private UpdateSeq committedUpdateSeq;
    private UpdateSeq pendingUpdateSeq;
    private UpdateSeq pendingPurgeSeq;
    private boolean dirty = false;

    FDBDirectorySearchHandler(final IndexWriter writer, final SearcherManager manager, final Analyzer analyzer) {
        super(analyzer);
        this.toString = String.format("FDBDirectorySearchHandler(%s)", writer.getDirectory());
        this.logger = LogManager.getLogger(writer.getDirectory().toString());
        this.writer = writer;
        this.manager = manager;
        this.committedUpdateSeq = getCommittedUpdateSeq(writer);
    }

    @Override
    public void close() throws IOException {
        this.manager.close();
        this.writer.rollback();
    }

    @Override
    public GroupSearchResponse groupSearch(final GroupSearchRequest request) throws IOException, ParseException {
        final int limit = request.getLimit();
        final boolean staleOk = request.getStale();
        final String groupBy = request.getGroupBy();
        final Sort groupSort = toSort(request);
        final int groupOffset = request.getGroupOffset();
        final int groupLimit = request.getGroupLimit();
        final Query query = parse(request.getQuery(), "");

        final GroupingSearch groupingSearch = new GroupingSearch(groupBy);
        if (groupSort != null) {
            groupingSearch.setGroupSort(groupSort);
        }
        groupingSearch.setCachingInMB(GROUP_CACHING_MB, true);
        groupingSearch.setAllGroups(false);
        groupingSearch.setGroupDocsLimit(defaultN(limit));

        return withSearcher(staleOk, searcher -> {
            final TopGroups<BytesRef> result = groupingSearch
                    .search(searcher, query, groupOffset, defaultN(groupLimit));
            final GroupSearchResponse.Builder responseBuilder = GroupSearchResponse.newBuilder();
            responseBuilder.setSeq(getUpdateSeq());
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
    public UpdateSeq getUpdateSeq() {
        UpdateSeq result = pendingUpdateSeq;
        if (result == null) {
            result = committedUpdateSeq;
        }
        return result;
    }

    @Override
    public InfoResponse info() throws IOException {
        final InfoResponse.Builder builder = InfoResponse.newBuilder();
        withSearcher(false, searcher -> {
            builder.setDocCount(searcher.getIndexReader().numDocs());
            builder.setDocDelCount(searcher.getIndexReader().numDeletedDocs());
            return null;
        });

        final Map<String, String> commitData = getLiveCommitData(writer);

        final String updateSeq = commitData.get("update_seq");
        if (updateSeq != null) {
            builder.setCommittedSeq(updateSeq);
        }

        final String purgeSeq = commitData.get("purge_seq");
        if (purgeSeq != null) {
            builder.setPurgeSeq(purgeSeq);
        }

        return builder.build();
    }

    @Override
    public void updateDocument(final DocumentUpdateRequest request) throws IOException {
        final String id = request.getId();
        if (id == null || id.isEmpty()) {
            throw new IllegalArgumentException("doc id is missing.");
        }

        final Document doc = toDoc(request);

        this.writer.updateDocument(new Term("_id", id), doc);
        if (request.hasSeq()) {
            this.pendingUpdateSeq = request.getSeq();
        }
        if (request.hasPurgeSeq()) {
            this.pendingPurgeSeq = request.getPurgeSeq();
        }
        this.dirty = true;
    }

    @Override
    public void deleteDocument(final DocumentDeleteRequest request) throws IOException {
        final String id = request.getId();
        if (id == null || id.isEmpty()) {
            throw new IllegalArgumentException("doc id is missing.");
        }

        this.writer.deleteDocuments(new Term("_id", id));
        if (request.hasSeq()) {
            this.pendingUpdateSeq = request.getSeq();
        }
        if (request.hasPurgeSeq()) {
            this.pendingPurgeSeq = request.getPurgeSeq();
        }
        this.dirty = true;
    }    

    @Override
    public void commit() throws IOException {
        final UpdateSeq committingSeq = pendingUpdateSeq;
        final UpdateSeq committingPurgeSeq = pendingPurgeSeq;
        if (dirty && (committingSeq != null || committingPurgeSeq != null)) {
            try {
                final Map<String, String> commitData = getLiveCommitData(writer);
                if (committingSeq != null) {
                    commitData.put("update_seq", committingSeq.getSeq());
                }
                if (committingPurgeSeq != null) {
                    commitData.put("purge_seq", committingPurgeSeq.getSeq());
                }
                this.writer.setLiveCommitData(commitData.entrySet());
                this.writer.commit();
                this.committedUpdateSeq = committingSeq;
                this.pendingUpdateSeq = null;
                this.pendingPurgeSeq = null;
                this.dirty = false;
                logger.info("committed: {}.", commitData);
            } catch (final IOException e) {
                logger.catching(e);
                throw e;
            }
        }
    }

    private UpdateSeq getCommittedUpdateSeq(final IndexWriter writer) {
        final String seq = getLiveCommitData(writer).get("update_seq");
        if (seq == null) {
            return null;
        }
        return UpdateSeq.newBuilder().setSeq(seq).build();
    }

    private static Map<String, String> getLiveCommitData(final IndexWriter writer) {
        final Map<String, String> result = new HashMap<String, String>();
        final Iterable<Entry<String, String>> it = writer.getLiveCommitData();
        if (it != null) {
            for (final Entry<String, String> entry : it) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
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
