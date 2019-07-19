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
import com.cloudant.search3.grpc.Search3.UpdateSeq;
import com.cloudant.search3.grpc.SearchGrpc;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

public final class Search extends SearchGrpc.SearchImplBase implements Closeable {

    private static final Logger LOGGER = LogManager.getLogger();

    private class CommitTask implements Runnable {

        private final Subspace index;
        private final SearchHandler handler;

        private CommitTask(final Subspace index, final SearchHandler handler) {
            this.index = index;
            this.handler = handler;
        }

        @Override
        public void run() {
            try {
                handler.commit();
            } catch (final IOException e) {
                LOGGER.warn("Closing failed handler for " + index, e);
                handlers.remove(index);
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
    private final int commitIntervalSecs;

    public static Search create(final Configuration config) throws Exception {
        // Initialize FDB.
        FDB.selectAPIVersion(config.getInt("fdb.version"));

        final Database db = FDB.instance().open();

        final SearchHandlerFactory searchHandlerFactory = (SearchHandlerFactory) Class
                .forName(config.getString("handler_factory")).newInstance();

        final ScheduledExecutorService scheduler = Executors
                .newScheduledThreadPool(config.getInt("scheduler_thread_count"));

        final Map<Subspace, SearchHandler> handlers = Collections
                .synchronizedMap(new SearchHandlerLRUMap(config.getInt("max_indexes_open")));

        final int commitIntervalSecs = config.getInt("commit_interval_secs");

        return new Search(db, searchHandlerFactory, scheduler, handlers, commitIntervalSecs);
    }

    private Search(final Database db, final SearchHandlerFactory searchHandlerFactory,
            final ScheduledExecutorService scheduler, final Map<Subspace, SearchHandler> handlers,
            final int commitIntervalSecs) {
        this.db = db;
        this.searchHandlerFactory = searchHandlerFactory;
        this.scheduler = scheduler;
        this.handlers = handlers;
        this.commitIntervalSecs = commitIntervalSecs;
    }

    @Override
    public void getUpdateSequence(final Index request, final StreamObserver<UpdateSeq> responseObserver) {
        try {
            final SearchHandler handler = getOrOpen(request);
            responseObserver.onNext(handler.getUpdateSeq());
            responseObserver.onCompleted();
        } catch (final IOException e) {
            LOGGER.catching(e);
            responseObserver.onError(fromThrowable(e));
        } catch (final RuntimeException e) {
            LOGGER.catching(e);
            responseObserver.onError(fromThrowable(e));
        }
    }

    @Override
    public void delete(final Index request, final StreamObserver<Empty> responseObserver) {
        try {
            final SearchHandler handler = remove(request);
            final Subspace subspace = toSubspace(request);
            handler.close();
            db.run(txn -> {
                txn.clear(subspace.range());
                return null;
            });
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
        try {
            final SearchHandler handler = getOrOpen(request);
            responseObserver.onNext(handler.info());
            responseObserver.onCompleted();
        } catch (final IOException e) {
            LOGGER.catching(e);
            responseObserver.onError(fromThrowable(e));
        } catch (final RuntimeException e) {
            LOGGER.catching(e);
            responseObserver.onError(fromThrowable(e));
        }
    }

    @Override
    public void search(final SearchRequest request, final StreamObserver<SearchResponse> responseObserver) {
        try {
            final SearchHandler handler = getOrOpen(request.getIndex());
            final SearchResponse response = handler.search(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (final IOException e) {
            LOGGER.catching(e);
            responseObserver.onError(fromThrowable(e));
        } catch (final ParseException e) {
            LOGGER.catching(e);
            responseObserver.onError(fromThrowable(e));
        } catch (final RuntimeException e) {
            LOGGER.catching(e);
            responseObserver.onError(fromThrowable(e));
        }
    }

    @Override
    public void groupSearch(
            final GroupSearchRequest request,
            final StreamObserver<GroupSearchResponse> responseObserver) {
        try {
            final SearchHandler handler = getOrOpen(request.getIndex());
            final GroupSearchResponse response = handler.groupSearch(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (final IOException e) {
            LOGGER.catching(e);
            responseObserver.onError(fromThrowable(e));
        } catch (final ParseException e) {
            LOGGER.catching(e);
            responseObserver.onError(fromThrowable(e));
        } catch (final RuntimeException e) {
            LOGGER.catching(e);
            responseObserver.onError(fromThrowable(e));
        }
    }

    @Override
    public void updateDocument(final DocumentUpdateRequest request, final StreamObserver<Empty> responseObserver) {
        try {
            final SearchHandler handler = getOrOpen(request.getIndex());
            handler.updateDocument(request);
            responseObserver.onNext(EMPTY);
            responseObserver.onCompleted();
        } catch (final IOException e) {
            LOGGER.catching(e);
            responseObserver.onError(fromThrowable(e));
        } catch (final RuntimeException e) {
            LOGGER.catching(e);
            responseObserver.onError(fromThrowable(e));
        }
    }

    @Override
    public void deleteDocument(final DocumentDeleteRequest request, final StreamObserver<Empty> responseObserver) {
        try {
            final SearchHandler handler = getOrOpen(request.getIndex());
            handler.deleteDocument(request);
            responseObserver.onNext(EMPTY);
            responseObserver.onCompleted();
        } catch (final IOException e) {
            LOGGER.catching(e);
            responseObserver.onError(fromThrowable(e));
        } catch (final RuntimeException e) {
            LOGGER.catching(e);
            responseObserver.onError(fromThrowable(e));
        }
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

    private SearchHandler getOrOpen(final Index index) throws IOException {
        final Subspace subspace = toSubspace(index);
        final Analyzer analyzer = SupportedAnalyzers.createAnalyzer(index);

        final SearchHandler handler = handlers.computeIfAbsent(subspace, key -> {
            try {
                final SearchHandler result = searchHandlerFactory.open(db, subspace, analyzer);
                scheduler.scheduleWithFixedDelay(
                        new CommitTask(subspace, result),
                        commitIntervalSecs,
                        commitIntervalSecs,
                        TimeUnit.SECONDS);
                LOGGER.info("Opened index {}", result);
                return result;
            } catch (final IOException e) {
                LOGGER.catching(e);
                return null;
            }
        });

        return handler;
    }

    private SearchHandler remove(final Index index) {
        return handlers.remove(toSubspace(index));
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

}
