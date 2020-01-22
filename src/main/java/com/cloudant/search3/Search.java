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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.jcs.utils.struct.LRUMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.BytesRef;

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
import com.cloudant.search3.grpc.Search3.SetUpdateSeqRequest;
import com.cloudant.search3.grpc.SearchGrpc;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

public final class Search extends SearchGrpc.SearchImplBase implements Closeable {

    @FunctionalInterface
    interface LuceneConsumer<T> {

        void accept(final T t) throws IOException, ParseException;

    }

    private static final Logger LOGGER = LogManager.getLogger();

    private class IdleExitTask implements Runnable {
        private final Subspace index;

        private IdleExitTask(final Subspace index) {
            this.index = index;
        }

        @Override
        public void run() {
            try {
                if (idle) {
                    throw new IdleHandlerException();
                }
                idle = true;
            } catch (final IOException e) {
                failedHandler(index, e);
                throw new RuntimeException(e);
            }
        }

    }

    private class CommitDirtyIndexTask implements Runnable {
        private final Subspace index;
        private final SearchHandler handler;

        private CommitDirtyIndexTask(final Subspace index, final SearchHandler handler) {
            this.index = index;
            this.handler = handler;
        }

        @Override
        public void run() {
            try {
                if (dirty && idle) {
                    idle = false;
                    handler.commit();
                    dirty = false;
                }
            } catch (final IOException e) {
                failedHandler(index, e);
                throw new RuntimeException(e);
            }
        }

    }

    private static class SearchHandlerLRUMap extends LRUMap<Subspace, SearchHandler> {

        public SearchHandlerLRUMap(int maxObjects) {
            super(maxObjects);
        }

        @Override
        protected void processRemovedLRU(Subspace key, SearchHandler value) {
            try {
                value.close();
                LOGGER.info("LRU closed {}", value);
            } catch (final IOException e) {
                LOGGER.warn("IOException when closing handler", e);
            }
        }
    }

    private static final Empty EMPTY = Empty.getDefaultInstance();

    private final Database db;
    private final ScheduledExecutorService scheduler;
    private final SearchHandlerFactory searchHandlerFactory;
    private final Map<Subspace, SearchHandler> handlers;
    private boolean idle = true;
    private boolean dirty = false;
    private final int tickerStartupDelaySecs = 10;
    private final int idleSearchExitSecs;
    private final int commitDirtyIntervalSecs;

    public static Search create(final Configuration config) throws Exception {
        // Initialize FDB.
        FDB.selectAPIVersion(config.getInt("fdb.version"));

        final Database db = FDB.instance().open();

        final SearchHandlerFactory searchHandlerFactory = (SearchHandlerFactory) Class
                .forName(config.getString("handler_factory")).newInstance();

        final ScheduledExecutorService scheduler = Executors
                .newScheduledThreadPool(config.getInt("scheduler_thread_count"));

        final Map<Subspace, SearchHandler> handlers = new SearchHandlerLRUMap(config.getInt("max_indexes_open"));

        final int idleSearchExitSecs = config.getInt("idle_search_exit_secs");
        final int commitDirtyIntervalSecs = config.getInt("commit_dirty_interval_secs");

        return new Search(db, searchHandlerFactory, scheduler, handlers, idleSearchExitSecs, commitDirtyIntervalSecs);
    }

    private Search(final Database db, final SearchHandlerFactory searchHandlerFactory,
            final ScheduledExecutorService scheduler, final Map<Subspace, SearchHandler> handlers,
            final int idleSearchExitSecs, final int commitDirtyIntervalSecs) {
        this.db = db;
        this.searchHandlerFactory = searchHandlerFactory;
        this.scheduler = scheduler;
        this.handlers = handlers;
        this.idleSearchExitSecs = idleSearchExitSecs;
        this.commitDirtyIntervalSecs = commitDirtyIntervalSecs;
    }

    @Override
    public void delete(final Index request, final StreamObserver<Empty> responseObserver) {
        final Subspace subspace = toSubspace(request);
        try {
            db.run(txn -> {
                txn.clear(subspace.range());
                return null;
            });
            final SearchHandler handler = handlers.remove(subspace);
            if (handler != null) {
                handler.close();
            }
            responseObserver.onNext(EMPTY);
            responseObserver.onCompleted();
            LOGGER.info("Deleted index {}.", subspace);
        } catch (final IOException e) {
            LOGGER.catching(e);
            responseObserver.onError(fromThrowable(e));
        } catch (final RuntimeException e) {
            LOGGER.catching(e);
            responseObserver.onError(fromThrowable(e));
        }
    }

    @Override
    public void info(final Index request, final StreamObserver<InfoResponse> responseObserver) {
        execute(request, responseObserver, handler -> {
            responseObserver.onNext(handler.info(request));
            responseObserver.onCompleted();
            idle = false;
        });
    }

    @Override
    public void setUpdateSequence(final SetUpdateSeqRequest request, final StreamObserver<SessionResponse> responseObserver) {
        execute(request.getIndex(), responseObserver, handler -> {
            final SessionResponse response = handler.setUpdateSeq(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            dirty = true;
            idle = false;
        });
    }

    @Override
    public void search(final SearchRequest request, final StreamObserver<SearchResponse> responseObserver) {
        execute(request.getIndex(), responseObserver, handler -> {
            final SearchResponse response = handler.search(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            idle = false;
        });
    }

    @Override
    public void groupSearch(
            final GroupSearchRequest request,
            final StreamObserver<GroupSearchResponse> responseObserver) {
        execute(request.getIndex(), responseObserver, handler -> {
            final GroupSearchResponse response = handler.groupSearch(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            idle = false;
        });
    }

    @Override
    public void updateDocument(final DocumentUpdateRequest request, final StreamObserver<SessionResponse> responseObserver) {
        execute(request.getIndex(), responseObserver, handler -> {
            final SessionResponse response = handler.updateDocument(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            idle = false;
            dirty = true;
        });
    }

    @Override
    public void deleteDocument(final DocumentDeleteRequest request, final StreamObserver<SessionResponse> responseObserver) {
        execute(request.getIndex(), responseObserver, handler -> {
            final SessionResponse response = handler.deleteDocument(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            idle = false;
            dirty = true;
        });
    }

    @Override
    public void analyze(final AnalyzeRequest request, final StreamObserver<AnalyzeResponse> responseObserver) {
        final Analyzer analyzer;
        try {
            analyzer = SupportedAnalyzers.single(request.getAnalyzer());
        } catch (final IllegalArgumentException e) {
            LOGGER.catching(e);
            responseObserver.onError(fromThrowable(e));
            return;
        }

        try (final TokenStream stream = analyzer.tokenStream(null, request.getText())) {
            stream.reset();
            final TermToBytesRefAttribute termAttribute = stream.getAttribute(TermToBytesRefAttribute.class);
            final AnalyzeResponse.Builder builder = AnalyzeResponse.newBuilder();
            while (stream.incrementToken()) {
                final BytesRef term = termAttribute.getBytesRef();
                builder.addTokens(term.utf8ToString());
            }
            stream.end();
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (final IOException e) {
            LOGGER.catching(e);
            responseObserver.onError(fromThrowable(e));
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
        handlers.forEach((index, handler) -> {
            try {
                handler.close();
            } catch (final IOException e) {
                LOGGER.catching(e);
            }
        });
        handlers.clear();
    }

    void commitAllHandlers() throws IOException {
        // This package-private method is intended for index commit test coverage.
        // It provides an easy interface for triggering commits to storage.
        for (SearchHandler h : handlers.values()) {
            try {
                h.commit();
            } catch (final IOException e) {
                LOGGER.catching(e);
                throw e;
            }
        }
    }

    private <T> void execute(
            final Index index,
            final StreamObserver<T> responseObserver,
            final LuceneConsumer<SearchHandler> f) {
        try {
            final SearchHandler handler = getOrOpen(index);
            f.accept(handler);
        } catch (final IOException | AlreadyClosedException e) {
            failedHandler(index, e);
            responseObserver.onError(fromThrowable(e));
        } catch (final ParseException e) {
            responseObserver.onError(fromThrowable(e));
        } catch (final SessionMismatchException e) {
            responseObserver.onError(fromThrowable(e));
        } catch (final RuntimeException e) {
            LOGGER.catching(e);
            responseObserver.onError(fromThrowable(e));
        }
    }

    private SearchHandler getOrOpen(final Index index) throws IOException {
        final Subspace subspace = toSubspace(index);
        final Analyzer analyzer = SupportedAnalyzers.createAnalyzer(index);

        try {
            return handlers.computeIfAbsent(subspace, key -> {
                try {
                    final SearchHandler result = searchHandlerFactory.open(db, subspace, analyzer);
                    scheduler.scheduleWithFixedDelay(
                            new IdleExitTask(subspace),
                            tickerStartupDelaySecs,
                            idleSearchExitSecs,
                            TimeUnit.SECONDS);
                    scheduler.scheduleWithFixedDelay(
                            new CommitDirtyIndexTask(subspace, result),
                            tickerStartupDelaySecs,
                            commitDirtyIntervalSecs,
                            TimeUnit.SECONDS);
                    LOGGER.info("Opened index {}", result);
                    return result;
                } catch (final IOException e) {
                    throw new FailedHandlerOpenException(e);
                }
            });
        } catch (final FailedHandlerOpenException e) {
            throw (IOException) e.getCause();
        }
    }

    private Subspace toSubspace(final Index index) {
        final ByteString prefix = index.getPrefix();
        if (prefix.isEmpty()) {
            throw new IllegalArgumentException("Index prefix not specified.");
        }
        return new Subspace(prefix.toByteArray());
    }

    private StatusException fromThrowable(final Throwable t) {
        final Status status;
        if (t instanceof ParseException) {
            status = Status.INVALID_ARGUMENT;
        } else if (t instanceof IOException) {
            status = Status.ABORTED;
        } else if (t instanceof IllegalArgumentException) {
            status = Status.INVALID_ARGUMENT;
        } else if (t instanceof IllegalStateException) {
            status = Status.FAILED_PRECONDITION;
        } else {
            status = Status.fromThrowable(t);
        }
        return status.withDescription(t.getMessage()).asException();
    }

    private void failedHandler(final Index index, final Exception e) {
        failedHandler(toSubspace(index), e);
    }

    private void failedHandler(final Subspace index, final Exception e) {
        final SearchHandler handler = handlers.remove(index);
        try {
            if (handler != null) {
                handler.close();
            }
            LOGGER.warn("Closed handler for index {} for reason {}.", index, e.getMessage());
        } catch (final IOException e1) {
            LOGGER.warn("Ignoring exception thrown while closing failed handler", e1);
        }

    }

}
