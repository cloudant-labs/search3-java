// Copyright 2019 IBM
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudant.search3;

import com.cloudant.fdblucene.FDBDirectory;
import com.cloudant.search3.grpc.Search3.DocumentDeleteRequest;
import com.cloudant.search3.grpc.Search3.DocumentUpdateRequest;
import com.cloudant.search3.grpc.Search3.Group;
import com.cloudant.search3.grpc.Search3.GroupSearchRequest;
import com.cloudant.search3.grpc.Search3.GroupSearchResponse;
import com.cloudant.search3.grpc.Search3.Hit;
import com.cloudant.search3.grpc.Search3.Index;
import com.cloudant.search3.grpc.Search3.InfoResponse;
import com.cloudant.search3.grpc.Search3.SessionResponse;
import com.cloudant.search3.grpc.Search3.UpdateSeq;
import io.prometheus.client.Histogram;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
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
import org.apache.lucene.util.IOUtils;

/** Provides all services for a specific index using an FDBDirectory; should be cached. */
public final class FDBDirectorySearchHandler extends BaseSearchHandler {

  private static final double GROUP_CACHING_MB = 4.0;

  private final String toString;
  private final FDBDirectory dir;
  private final IndexWriter writer;
  private final SearcherManager manager;
  private UpdateSeq pendingUpdateSeq;
  private UpdateSeq pendingPurgeSeq;

  FDBDirectorySearchHandler(
      final FDBDirectory dir,
      final IndexWriter writer,
      final SearcherManager manager,
      final Analyzer analyzer) {
    super(analyzer);
    this.toString = String.format("FDBDirectorySearchHandler(%s)", writer.getDirectory());
    this.logger = LogManager.getLogger(dir.toString());
    this.dir = dir;
    this.writer = writer;
    this.manager = manager;
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(this.manager, this.writer, this.dir);
  }

  @Override
  public GroupSearchResponse groupSearch(final GroupSearchRequest request)
      throws IOException, ParseException {
    verifySession(request.getIndex());
    final int limit = request.getLimit();
    final boolean staleOk = request.getStale();
    final String groupBy = validateGroupField(request.getGroupBy());
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
    groupingSearch.setGroupDocsLimit(limit);

    return withSearcher(
        staleOk,
        searcher -> {
          final TopGroups<BytesRef> result;
          final Histogram.Timer requestTimer = LATENCIES.labels("searches").startTimer();
          try {
            result = groupingSearch.search(searcher, query, groupOffset, groupLimit);
          } finally {
            requestTimer.observeDuration();
          }
          final GroupSearchResponse.Builder responseBuilder = GroupSearchResponse.newBuilder();
          responseBuilder.setSession(getSession());
          responseBuilder.setMatches(result.totalHitCount);
          responseBuilder.setGroupMatches(result.totalGroupedHitCount);

          for (final GroupDocs<BytesRef> group : result.groups) {
            final Group.Builder groupBuilder = Group.newBuilder();
            groupBuilder.setMatches(group.totalHits.value);
            if (group.groupValue != null) {
              groupBuilder.setBy(group.groupValue.utf8ToString());
            }
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
  public InfoResponse info(final Index request) throws IOException {
    verifySession(request);
    final InfoResponse.Builder builder = InfoResponse.newBuilder();
    withSearcher(
        false,
        searcher -> {
          builder.setDocCount(searcher.getIndexReader().numDocs());
          builder.setDocDelCount(searcher.getIndexReader().numDeletedDocs());
          return null;
        });
    builder.setSession(getSession());

    final UpdateSeq pendingUpdateSeq = this.pendingUpdateSeq;
    if (pendingUpdateSeq != null) {
      builder.setPendingSeq(pendingUpdateSeq);
    }

    final UpdateSeq pendingPurgeSeq = this.pendingPurgeSeq;
    if (pendingPurgeSeq != null) {
      builder.setPendingPurgeSeq(pendingPurgeSeq);
    }

    final Map<String, String> commitData = getLiveCommitData(writer);

    final String updateSeq = commitData.get("update_seq");
    if (updateSeq != null) {
      builder.setCommittedSeq(seq(updateSeq));
    }

    final String purgeSeq = commitData.get("purge_seq");
    if (purgeSeq != null) {
      builder.setPurgeSeq(seq(purgeSeq));
    }

    return builder.build();
  }

  @Override
  public SessionResponse updateDocument(final DocumentUpdateRequest request) throws IOException {
    verifySession(request.getIndex());
    final String id = request.getId();
    if (id == null || id.isEmpty()) {
      throw new IllegalArgumentException("doc id is missing.");
    }
    if (!request.hasSeq() && !request.hasPurgeSeq()) {
      throw new IllegalArgumentException("Must set at least one seq parameter.");
    }

    final Document doc = toDoc(request);

    final Histogram.Timer requestTimer = LATENCIES.labels("updates").startTimer();
    try {
      this.writer.updateDocument(new Term("_id", id), doc);
    } finally {
      requestTimer.observeDuration();
    }
    if (request.hasSeq()) {
      this.pendingUpdateSeq = request.getSeq();
    }
    if (request.hasPurgeSeq()) {
      this.pendingPurgeSeq = request.getPurgeSeq();
    }
    return sessionResponse();
  }

  @Override
  public SessionResponse deleteDocument(final DocumentDeleteRequest request) throws IOException {
    verifySession(request.getIndex());
    final String id = request.getId();
    if (id == null || id.isEmpty()) {
      throw new IllegalArgumentException("doc id is missing.");
    }
    if (!request.hasSeq() && !request.hasPurgeSeq()) {
      throw new IllegalArgumentException("Must set at least one seq parameter.");
    }

    final Histogram.Timer requestTimer = LATENCIES.labels("deletes").startTimer();
    try {
      this.writer.deleteDocuments(new Term("_id", id));
    } finally {
      requestTimer.observeDuration();
    }
    if (request.hasSeq()) {
      this.pendingUpdateSeq = request.getSeq();
    }
    if (request.hasPurgeSeq()) {
      this.pendingPurgeSeq = request.getPurgeSeq();
    }
    return sessionResponse();
  }

  @Override
  public void commit() throws IOException {
    final UpdateSeq committingSeq = pendingUpdateSeq;
    final UpdateSeq committingPurgeSeq = pendingPurgeSeq;
    if (committingSeq != null || committingPurgeSeq != null) {
      try {
        final Map<String, String> commitData = getLiveCommitData(writer);
        if (committingSeq != null) {
          commitData.put("update_seq", committingSeq.getSeq());
        }
        if (committingPurgeSeq != null) {
          commitData.put("purge_seq", committingPurgeSeq.getSeq());
        }
        this.writer.setLiveCommitData(commitData.entrySet());
        final Histogram.Timer requestTimer = LATENCIES.labels("commits").startTimer();
        try {
          this.writer.commit();
        } finally {
          requestTimer.observeDuration();
        }
        this.pendingUpdateSeq = null;
        this.pendingPurgeSeq = null;
        logger.info("committed: {}.", commitData);
      } catch (final IOException e) {
        logger.catching(e);
        throw e;
      }
    }
  }

  @Override
  public boolean hasUncommittedChanges() {
    return this.pendingUpdateSeq != null || this.pendingPurgeSeq != null;
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
  protected <T> T withSearcher(final boolean staleOk, final IOFunction<IndexSearcher, T> f)
      throws IOException {
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

  protected String getSession() {
    // Demeter? I hardly know her.
    return dir.getUUID().toString();
  }

  private String validateGroupField(final String field) throws ParseException {
    final Matcher m = SORT_FIELD_RE.matcher(field);
    if (m.matches()) {
      final String fieldTypeStr = m.group(3) == null ? "string" : m.group(3);
      switch (fieldTypeStr) {
        case "string":
          return m.group(2);
        case "number":
          throw new ParseException("Group by number not supported. Group by string terms only.");
      }
    }
    throw new ParseException("Unrecognized group_field parameter: " + field);
  }

  @Override
  public String toString() {
    return toString;
  }
}
