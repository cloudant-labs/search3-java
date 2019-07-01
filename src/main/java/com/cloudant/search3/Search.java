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

import static com.cloudant.search3.Converters.toDoc;
import static com.cloudant.search3.Converters.toFieldSet;
import static com.cloudant.search3.Converters.toQuery;
import static com.cloudant.search3.Converters.toSort;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.util.BytesRef;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.subspace.Subspace;
import com.cloudant.search3.grpc.Search3.DocumentUpdate;
import com.cloudant.search3.grpc.Search3.GroupSearchRequest;
import com.cloudant.search3.grpc.Search3.GroupSearchResponse;
import com.cloudant.search3.grpc.Search3.Index;
import com.cloudant.search3.grpc.Search3.InfoResponse;
import com.cloudant.search3.grpc.Search3.SearchRequest;
import com.cloudant.search3.grpc.Search3.SearchResponse;
import com.cloudant.search3.grpc.Search3.ServiceResponse;
import com.cloudant.search3.grpc.Search3.SetUpdateSeq;
import com.cloudant.search3.grpc.Search3.UpdateSeq;
import com.cloudant.search3.grpc.SearchGrpc;
import com.google.protobuf.ByteString;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

public final class Search extends SearchGrpc.SearchImplBase {

    private static final ServiceResponse OK = ServiceResponse.newBuilder().setCode(0).build();

    private final Database db;
    private final SearchHandlerFactory searchHandlerFactory;
    private final ConcurrentMap<Subspace, SearchHandler> handlers = new ConcurrentHashMap<Subspace, SearchHandler>();

    public Search(final Database db, final SearchHandlerFactory searchHandlerFactory) {
        this.db = db;
        this.searchHandlerFactory = searchHandlerFactory;
    }

    @Override
    public void delete(final Index request, final StreamObserver<ServiceResponse> responseObserver) {
        final Subspace subspace;
        try {
            final SearchHandler handler = getOrOpen(request);
            subspace = toSubspace(request);
            handler.close();
            db.run(txn -> {
                txn.clear(subspace.range());
                return null;
            });
            responseObserver.onNext(OK);
            responseObserver.onCompleted();
        } catch (final IOException e) {
            responseObserver.onError(Status.fromThrowable(e).asException());
        }
    }

    @Override
    public void getUpdateSequence(final Index request, final StreamObserver<UpdateSeq> responseObserver) {
        try {
            final SearchHandler handler = getOrOpen(request);
            final UpdateSeq updateSeq = UpdateSeq.newBuilder().setSeq(handler.getUpdateSeq()).build();
            responseObserver.onNext(updateSeq);
            responseObserver.onCompleted();
        } catch (final IOException e) {
            responseObserver.onError(Status.fromThrowable(e).asException());
        }
    }

    @Override
    public void info(final Index request, final StreamObserver<InfoResponse> responseObserver) {
        throw new UnsupportedOperationException("info not supported.");
    }

    @Override
    public void search(final SearchRequest request, final StreamObserver<SearchResponse> responseObserver) {
        try {
            final Query query = toQuery(request);
            final int limit = request.getLimit();
            final Set<String> fieldsToLoad = toFieldSet(request);
            final boolean staleOk = request.getStale();

            final SearchHandler handler = getOrOpen(request.getIndex());

            final SearchResponse response;
            if (request.hasSort()) {
                final Sort sort = toSort(request);
                response = handler.search(query, limit, sort, fieldsToLoad, staleOk);
            } else {
                response = handler.search(query, limit, fieldsToLoad, staleOk);
            }
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (final IOException e) {
            responseObserver.onError(Status.fromThrowable(e).asException());
        } catch (final ParseException e) {
            responseObserver.onError(Status.fromThrowable(e).asException());
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
            final TopGroups<BytesRef> result = handler
                    .groupingSearch(query, groupBy, groupSort, groupOffset, groupLimit, limit, staleOk);

            final GroupSearchResponse response = GroupSearchResponse.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (final IOException e) {
            responseObserver.onError(Status.fromThrowable(e).asException());
        } catch (final ParseException e) {
            responseObserver.onError(Status.fromThrowable(e).asException());
        }
    }

    @Override
    public void setUpdateSequence(final SetUpdateSeq request, final StreamObserver<ServiceResponse> responseObserver) {
        try {
            final SearchHandler handler = getOrOpen(request.getIndex());
            handler.setPendingUpdateSeq(request.getSeq());
        } catch (final IOException e) {
            responseObserver.onError(Status.fromThrowable(e).asException());
        }
        responseObserver.onNext(OK);
        responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<DocumentUpdate> update(final StreamObserver<ServiceResponse> responseObserver) {
        return new StreamObserver<DocumentUpdate>() {

            @Override
            public void onNext(final DocumentUpdate request) {
                try {
                    final SearchHandler handler = getOrOpen(request.getIndex());

                    final String id = request.getId();
                    if (id == null || id.isEmpty()) {
                        throw new IOException("doc id is missing.");
                    }
                    final Term idTerm = new Term("_id", id);

                    if (request.getFieldsCount() == 0) {
                        handler.deleteDocuments(idTerm);
                    } else {
                        final Document doc = toDoc(request);
                        handler.updateDocument(idTerm, doc);
                    }
                } catch (final IOException e) {
                    responseObserver.onError(Status.fromThrowable(e).asException());
                }
            }

            @Override
            public void onError(final Throwable t) {
                // No-op.
            }

            @Override
            public void onCompleted() {
                responseObserver.onNext(OK);
                responseObserver.onCompleted();
            }

        };
    }

    private SearchHandler getOrOpen(final Index index) throws IndexNotFoundException {
        final Subspace indexSubspace = toSubspace(index);
        final SearchHandler result = handlers.computeIfAbsent(indexSubspace, key -> {
            try {
                return searchHandlerFactory.open(db, key, new StandardAnalyzer());
            } catch (final IOException e) {
                return null;
            }
        });
        if (result == null) {
            throw new IndexNotFoundException(index + " is not an index.");
        }
        return result;
    }

    private Subspace toSubspace(final Index index) throws IndexNotFoundException {
        final ByteString prefix = index.getPrefix();
        if (prefix.isEmpty()) {
            throw new IndexNotFoundException("Index not specified by prefix.");
        }
        return new Subspace(prefix.toByteArray());
    }

}
