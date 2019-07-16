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

import static com.cloudant.search3.Converters.toAfter;
import static com.cloudant.search3.Converters.toDoc;
import static com.cloudant.search3.Converters.toFieldSet;
import static com.cloudant.search3.Converters.toSort;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.jcs.utils.struct.LRUMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.subspace.Subspace;
import com.cloudant.search3.grpc.Search3.DocumentDelete;
import com.cloudant.search3.grpc.Search3.DocumentUpdate;
import com.cloudant.search3.grpc.Search3.GroupSearchRequest;
import com.cloudant.search3.grpc.Search3.GroupSearchResponse;
import com.cloudant.search3.grpc.Search3.Index;
import com.cloudant.search3.grpc.Search3.InfoResponse;
import com.cloudant.search3.grpc.Search3.OpenIndex;
import com.cloudant.search3.grpc.Search3.SearchRequest;
import com.cloudant.search3.grpc.Search3.SearchResponse;
import com.cloudant.search3.grpc.Search3.SetUpdateSeq;
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
    public void getUpdateSequence(Index request, StreamObserver<UpdateSeq> responseObserver) {
        try {
            final SearchHandler handler = openExisting(request);
            final String result = handler.getUpdateSeq();
            final UpdateSeq updateSeq = UpdateSeq.newBuilder().setSeq(result).build();
            responseObserver.onNext(updateSeq);
            responseObserver.onCompleted();
        } catch (final IOException e) {
            responseObserver.onError(fromThrowable(e));
        } catch (final RuntimeException e) {
            responseObserver.onError(fromThrowable(e));
        }
    }

    @Override
    public void setUpdateSequence(SetUpdateSeq request, StreamObserver<Empty> responseObserver) {
        try {
            final SearchHandler handler = openExisting(request.getIndex());
            handler.setPendingUpdateSeq(request.getSeq());
            responseObserver.onNext(EMPTY);
            responseObserver.onCompleted();
        } catch (final IOException e) {
            responseObserver.onError(fromThrowable(e));
        } catch (final RuntimeException e) {
            responseObserver.onError(fromThrowable(e));
        }
    }

    @Override
    public void open(final OpenIndex request, final StreamObserver<Empty> responseObserver) {
        final Analyzer analyzer = SupportedAnalyzers.createAnalyzer(request);
        final Subspace subspace = toSubspace(request.getIndex());
        SearchHandler handler;
        try {
            handler = searchHandlerFactory.open(db, subspace, analyzer);
            LOGGER.info("Opened index {}", handler);
        } catch (final IOException e) {
            responseObserver.onError(fromThrowable(e));
            return;
        }
        final SearchHandler oldHandler = handlers.put(subspace, handler);
        if (oldHandler != null) {
            try {
                oldHandler.close();
            } catch (final IOException e) {
                LOGGER.warn("IOException when closing old handler", e);
            }
        }
        scheduler.scheduleWithFixedDelay(
                new CommitTask(subspace, handler),
                commitIntervalSecs,
                commitIntervalSecs,
                TimeUnit.SECONDS);
        responseObserver.onNext(EMPTY);
        responseObserver.onCompleted();
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
            responseObserver.onError(fromThrowable(e));
        } catch (final RuntimeException e) {
            responseObserver.onError(fromThrowable(e));
        }
    }

    @Override
    public void info(final Index request, final StreamObserver<InfoResponse> responseObserver) {
        try {
            final SearchHandler handler = openExisting(request);
            responseObserver.onNext(handler.info());
            responseObserver.onCompleted();
        } catch (final IOException e) {
            responseObserver.onError(fromThrowable(e));
        } catch (final RuntimeException e) {
            responseObserver.onError(fromThrowable(e));
        }
    }

    @Override
    public void search(final SearchRequest request, final StreamObserver<SearchResponse> responseObserver) {
        try {
            final int limit = request.getLimit();
            final Set<String> fieldsToLoad = toFieldSet(request);
            final boolean staleOk = request.getStale();
            final Sort sort = toSort(request);
            final ScoreDoc after = toAfter(request, sort);

            final SearchHandler handler = openExisting(request.getIndex());
            final Query query = handler.parse(request.getQuery(), request.getPartition());
            final SearchResponse response = handler.search(after, query, limit, sort, fieldsToLoad, staleOk);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (final IOException e) {
            responseObserver.onError(fromThrowable(e));
        } catch (final ParseException e) {
            responseObserver.onError(fromThrowable(e));
        } catch (final RuntimeException e) {
            responseObserver.onError(fromThrowable(e));
        }
    }

    @Override
    public void groupSearch(
            final GroupSearchRequest request,
            final StreamObserver<GroupSearchResponse> responseObserver) {
        try {
            final int limit = request.getLimit();
            final boolean staleOk = request.getStale();
            final String groupBy = request.getGroupBy();
            final Sort groupSort = toSort(request);
            final int groupOffset = request.getGroupOffset();
            final int groupLimit = request.getGroupLimit();

            final SearchHandler handler = openExisting(request.getIndex());
            final Query query = handler.parse(request.getQuery(), "");
            final GroupSearchResponse response = handler
                    .groupingSearch(query, groupBy, groupSort, groupOffset, groupLimit, limit, staleOk);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (final IOException e) {
            responseObserver.onError(fromThrowable(e));
        } catch (final ParseException e) {
            responseObserver.onError(fromThrowable(e));
        } catch (final RuntimeException e) {
            responseObserver.onError(fromThrowable(e));
        }
    }

    @Override
    public void updateDocument(final DocumentUpdate request, final StreamObserver<Empty> responseObserver) {
        try {
            final SearchHandler handler = openExisting(request.getIndex());
            final String id = request.getId();
            if (id == null || id.isEmpty()) {
                throw new IOException("doc id is missing.");
            }
            final Term idTerm = new Term("_id", id);
            final Document doc = toDoc(request);
            handler.updateDocument(idTerm, doc);
            responseObserver.onNext(EMPTY);
            responseObserver.onCompleted();
        } catch (final IOException e) {
            responseObserver.onError(fromThrowable(e));
        } catch (final RuntimeException e) {
            responseObserver.onError(fromThrowable(e));
        }
    }

    @Override
    public void deleteDocument(final DocumentDelete request, final StreamObserver<Empty> responseObserver) {
        try {
            final SearchHandler handler = openExisting(request.getIndex());
            final String id = request.getId();
            if (id == null || id.isEmpty()) {
                throw new IOException("doc id is missing.");
            }
            final Term idTerm = new Term("_id", id);
            handler.deleteDocument(idTerm);
            responseObserver.onNext(EMPTY);
            responseObserver.onCompleted();
        } catch (final IOException e) {
            responseObserver.onError(fromThrowable(e));
        } catch (final RuntimeException e) {
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

    private SearchHandler openExisting(final Index index) throws IOException {
        final Subspace indexSubspace = toSubspace(index);
        final SearchHandler result = handlers.get(indexSubspace);
        if (result == null) {
            throw new IllegalStateException("index not opened");
        }
        return result;
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
        } else if (t instanceof IllegalStateException) {
            status = Status.FAILED_PRECONDITION;
        } else {
            status = Status.fromThrowable(t);
        }
        return status.withDescription(t.getMessage()).asException();
    }

}
