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

import static java.util.concurrent.TimeUnit.SECONDS;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.subspace.Subspace;
import com.cloudant.search3.grpc.Search3.AnalyzeRequest;
import com.cloudant.search3.grpc.Search3.AnalyzeResponse;
import com.cloudant.search3.grpc.Search3.DocumentDeleteRequest;
import com.cloudant.search3.grpc.Search3.DocumentUpdateRequest;
import com.cloudant.search3.grpc.Search3.GroupSearchRequest;
import com.cloudant.search3.grpc.Search3.GroupSearchResponse;
import com.cloudant.search3.grpc.Search3.Index;
import com.cloudant.search3.grpc.Search3.InfoResponse;
import com.cloudant.search3.grpc.Search3.SearchRequest;
import com.cloudant.search3.grpc.Search3.SearchResponse;
import com.cloudant.search3.grpc.Search3.SessionResponse;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.protobuf.ByteString;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.commons.configuration2.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.BytesRef;

public final class Search implements Closeable {

  @FunctionalInterface
  interface LuceneFunction<T, R> {

    R apply(final T t) throws IOException, ParseException;
  }

  private static final Logger LOGGER = LogManager.getLogger();

  private static class CleanupTask implements Runnable {

    private final LoadingCache<?, ?> cache;

    private CleanupTask(final LoadingCache<?, ?> cache) {
      this.cache = cache;
    }

    public void run() {
      cache.cleanUp();
    }
  }

  private class CommitTask implements Runnable {

    private final SearchCacheKey key;
    private final SearchHandler handler;

    private CommitTask(final SearchCacheKey key, final SearchHandler handler) {
      this.key = key;
      this.handler = handler;
    }

    @Override
    public void run() {
      try {
        if (handler.hasUncommittedChanges()) {
          handler.commit();
        }
      } catch (final IOException e) {
        LOGGER.warn("I/O exception for index " + key + " while committing", e);
        handlers.invalidate(key);
      }
    }
  }

  /**
   * Only use the index prefix for equality check so that an Index message with and without a
   * session are considered the same.
   */
  private static class SearchCacheKey {

    private final Index index;

    private SearchCacheKey(final Index index) {
      this.index = index;
    }

    @Override
    public int hashCode() {
      final ByteString prefix = index.getPrefix();
      final int prime = 31;
      int result = 1;
      result = prime * result + ((prefix == null) ? 0 : prefix.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      SearchCacheKey other = (SearchCacheKey) obj;
      if (index.getPrefix() == null) {
        if (other.index.getPrefix() != null) return false;
      } else if (!index.getPrefix().equals(other.index.getPrefix())) return false;
      return true;
    }

    @Override
    public String toString() {
      return index.toString();
    }
  }

  private class SearchCacheLoader extends CacheLoader<SearchCacheKey, SearchHandler> {

    @Override
    public SearchHandler load(final SearchCacheKey key) throws Exception {
      final Subspace subspace = toSubspace(key.index);
      final Analyzer analyzer = SupportedAnalyzers.createAnalyzer(key.index);
      final SearchHandler result = searchHandlerFactory.open(db, subspace, analyzer);
      final ScheduledFuture<?> commitFuture =
          scheduler.scheduleWithFixedDelay(
              new CommitTask(key, result),
              commitIntervalSecs,
              commitIntervalSecs,
              TimeUnit.SECONDS);
      commitFutures.put(key, commitFuture);
      LOGGER.info("Opened index {}", result);
      return result;
    }
  }

  private class SearchRemovalListener implements RemovalListener<SearchCacheKey, SearchHandler> {

    @Override
    public void onRemoval(final RemovalNotification<SearchCacheKey, SearchHandler> notification) {
      try {
        commitFutures.remove(notification.getKey()).cancel(false);
        notification.getValue().close();
        LOGGER.info(
            "Closed handler for index {} for reason {}.",
            notification.getValue(),
            notification.getCause());
      } catch (final IOException e) {
        LOGGER.error("I/O exception while closing evicted index " + notification.getValue(), e);
      }
    }
  }

  private final Database db;
  private final ScheduledExecutorService scheduler;
  private final SearchHandlerFactory searchHandlerFactory;
  private final LoadingCache<SearchCacheKey, SearchHandler> handlers;
  private final Map<SearchCacheKey, ScheduledFuture<?>> commitFutures;
  private final int commitIntervalSecs;

  public Search(final Configuration config) throws Exception {
    this.db = FDB.instance().open();
    this.searchHandlerFactory =
        (SearchHandlerFactory) Class.forName(config.getString("handler_factory")).newInstance();
    this.scheduler = Executors.newScheduledThreadPool(config.getInt("scheduler_thread_count", 8));
    this.handlers =
        CacheBuilder.from(config.getString("cache_config", ""))
            .removalListener(new SearchRemovalListener())
            .build(new SearchCacheLoader());
    this.commitIntervalSecs = config.getInt("commit_interval_secs", 5);
    this.commitFutures = new ConcurrentHashMap<SearchCacheKey, ScheduledFuture<?>>();
    scheduler.scheduleWithFixedDelay(
        new CleanupTask(handlers), commitIntervalSecs, commitIntervalSecs, SECONDS);
  }

  public void delete(final Index request) throws IOException {
    final Subspace subspace = toSubspace(request);
    db.run(
        txn -> {
          txn.clear(subspace.range());
          return null;
        });
    handlers.invalidate(new SearchCacheKey(request));
  }

  public InfoResponse info(final Index request) throws Exception {
    return execute(
        request,
        handler -> {
          final InfoResponse response = handler.info(request);
          return response;
        });
  }

  public SearchResponse search(final SearchRequest request) throws Exception {
    return execute(
        request.getIndex(),
        handler -> {
          return handler.search(request);
        });
  }

  public GroupSearchResponse groupSearch(final GroupSearchRequest request) throws Exception {
    return execute(
        request.getIndex(),
        handler -> {
          return handler.groupSearch(request);
        });
  }

  public SessionResponse updateDocument(final DocumentUpdateRequest request) throws Exception {
    return execute(
        request.getIndex(),
        handler -> {
          return handler.updateDocument(request);
        });
  }

  public SessionResponse deleteDocument(final DocumentDeleteRequest request) throws Exception {
    return execute(
        request.getIndex(),
        handler -> {
          return handler.deleteDocument(request);
        });
  }

  public AnalyzeResponse analyze(final AnalyzeRequest request) throws IOException {
    final Analyzer analyzer;
    try {
      analyzer = SupportedAnalyzers.single(request.getAnalyzer());
    } catch (final IllegalArgumentException e) {
      LOGGER.catching(e);
      throw e;
    }

    try (final TokenStream stream = analyzer.tokenStream(null, request.getText())) {
      stream.reset();
      final TermToBytesRefAttribute termAttribute =
          stream.getAttribute(TermToBytesRefAttribute.class);
      final AnalyzeResponse.Builder builder = AnalyzeResponse.newBuilder();
      while (stream.incrementToken()) {
        final BytesRef term = termAttribute.getBytesRef();
        builder.addTokens(term.utf8ToString());
      }
      stream.end();
      return builder.build();
    } catch (final IOException e) {
      LOGGER.catching(e);
      throw e;
    }
  }

  @Override
  public void close() {
    scheduler.shutdownNow();
    try {
      scheduler.awaitTermination(10, TimeUnit.SECONDS);
    } catch (final InterruptedException e) {
      // Ignored.
    }
    handlers.invalidateAll();
  }

  private <R> R execute(final Index index, final LuceneFunction<SearchHandler, R> f)
      throws IOException, ParseException, ExecutionException {
    final SearchCacheKey key = new SearchCacheKey(index);
    try {
      final SearchHandler handler = handlers.get(key);
      return f.apply(handler);
    } catch (final IOException | AlreadyClosedException | ExecutionException e) {
      LOGGER.warn(
          "Invalidating handler for index {} for reason {}.", toSubspace(index), e.getMessage());
      handlers.invalidate(key);
      throw e;
    } catch (final RuntimeException e) {
      LOGGER.catching(e);
      throw e;
    }
  }

  private static Subspace toSubspace(final Index index) {
    final ByteString prefix = index.getPrefix();
    if (prefix.isEmpty()) {
      throw new IllegalArgumentException("Index prefix not specified.");
    }
    return new Subspace(prefix.toByteArray());
  }
}
