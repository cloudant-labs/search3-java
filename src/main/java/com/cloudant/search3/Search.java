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
import static com.cloudant.search3.Converters.toQuery;
import static com.cloudant.search3.Converters.toSort;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.subspace.Subspace;
import com.cloudant.search3.grpc.Search3.DocumentDelete;
import com.cloudant.search3.grpc.Search3.DocumentUpdate;
import com.cloudant.search3.grpc.Search3.GroupSearchRequest;
import com.cloudant.search3.grpc.Search3.GroupSearchResponse;
import com.cloudant.search3.grpc.Search3.Index;
import com.cloudant.search3.grpc.Search3.InfoResponse;
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

    private static final int SCHEDULER_THREAD_COUNT = 8;

    private static final int MAX_INDEXES_OPEN = 1000;

    private static final int COMMIT_INTERVAL_SECS = 5;

    private static final Logger LOGGER = LogManager.getLogger();

    private class LRU<K, V> extends LinkedHashMap<K, V> {

        private static final long serialVersionUID = 1L;
        private final int capacity;

        private LRU(final int capacity) {
            super(16, 0.75f, true);
            this.capacity = capacity;
        }

        @Override
        protected boolean removeEldestEntry(final Entry<K, V> eldest) {
            final boolean result = size() > capacity;
            if (result) {
                LOGGER.info("{} evicted by LRU", eldest.getKey());
            }
            return result;
        }

    }

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

    private static final Empty EMPTY = Empty.getDefaultInstance();

    private final Database db;
    private final SearchHandlerFactory searchHandlerFactory;
    private final Map<Subspace, SearchHandler> handlers = new LRU<Subspace, SearchHandler>(MAX_INDEXES_OPEN);
    private final ScheduledExecutorService scheduler;

    public Search(final Database db, final SearchHandlerFactory searchHandlerFactory) {
        this.db = db;
        this.searchHandlerFactory = searchHandlerFactory;
        this.scheduler = Executors.newScheduledThreadPool(SCHEDULER_THREAD_COUNT);
    }

    @Override
    public void getUpdateSequence(Index request, StreamObserver<UpdateSeq> responseObserver) {
        try {
            final SearchHandler handler = getOrOpen(request);
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
            final SearchHandler handler = getOrOpen(request.getIndex());
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
    public void delete(final Index request, final StreamObserver<Empty> responseObserver) {
        final Subspace subspace;
        try {
            final SearchHandler handler = remove(request);
            subspace = toSubspace(request);
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
            final SearchHandler handler = getOrOpen(request);
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
            final Query query = toQuery(request);
            final int limit = request.getLimit();
            final Set<String> fieldsToLoad = toFieldSet(request);
            final boolean staleOk = request.getStale();
            final Sort sort = toSort(request);
            final ScoreDoc after = toAfter(request, sort);

            final SearchHandler handler = getOrOpen(request.getIndex());
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
            final Query query = toQuery(request);
            final int limit = request.getLimit();
            final boolean staleOk = request.getStale();
            final String groupBy = request.getGroupBy();
            final Sort groupSort = toSort(request);
            final int groupOffset = request.getGroupOffset();
            final int groupLimit = request.getGroupLimit();

            final SearchHandler handler = getOrOpen(request.getIndex());
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
            final SearchHandler handler = getOrOpen(request.getIndex());
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
            final SearchHandler handler = getOrOpen(request.getIndex());
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

    private SearchHandler getOrOpen(final Index index) throws IOException {
        final Subspace indexSubspace = toSubspace(index);
        synchronized (handlers) {
            SearchHandler result = handlers.get(indexSubspace);
            if (result == null) {
                result = searchHandlerFactory.open(db, indexSubspace, new StandardAnalyzer());
                LOGGER.info("Opened index {}.", result);
                scheduler.scheduleWithFixedDelay(
                        new CommitTask(indexSubspace, result),
                        COMMIT_INTERVAL_SECS,
                        COMMIT_INTERVAL_SECS,
                        TimeUnit.SECONDS);
            }

            // Insert or update.
            handlers.put(indexSubspace, result);
            return result;
        }
    }

    private SearchHandler remove(final Index index) {
        final Subspace indexSubspace = toSubspace(index);
        return handlers.remove(indexSubspace);
    }

    private Subspace toSubspace(final Index index) {
        final ByteString prefix = index.getPrefix();
        if (prefix.isEmpty()) {
            throw new IllegalArgumentException("Index prefix not specified.");
        }
        return new Subspace(prefix.toByteArray());
    }

    private StatusException fromThrowable(final Throwable t) {
        if (t instanceof ParseException) {
            return Status.INVALID_ARGUMENT.withDescription(t.getMessage()).asException();
        }
        if (t instanceof IOException) {
            return Status.ABORTED.withDescription(t.getMessage()).asException();
        }
        return Status.fromThrowable(t).withDescription(t.getMessage()).asException();
    }

}
