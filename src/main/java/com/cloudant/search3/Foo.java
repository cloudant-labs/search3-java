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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.util.BytesRef;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.subspace.Subspace;
import com.cloudant.search3.grpc.Search3.DocumentUpdate;
import com.cloudant.search3.grpc.Search3.Index;
import com.cloudant.search3.grpc.Search3.InfoResponse;
import com.cloudant.search3.grpc.Search3.SearchRequest;
import com.cloudant.search3.grpc.Search3.SearchResponse;
import com.cloudant.search3.grpc.Search3.ServiceResponse;
import com.cloudant.search3.grpc.Search3.SetUpdateSeq;
import com.cloudant.search3.grpc.Search3.UpdateSeq;
import com.cloudant.search3.grpc.SearchGrpc;

import io.grpc.stub.StreamObserver;

public final class Foo extends SearchGrpc.SearchImplBase {

    private final Database db;
    private final Analyzer analyzer = new StandardAnalyzer(); // TODO specify by index.
    private final QueryParser queryParser = new QueryParser("default", analyzer);
    private final ConcurrentMap<Subspace, SearchHandler> handlers = new ConcurrentHashMap<Subspace, SearchHandler>();

    public Foo(final Database db) {
        this.db = db;
    }

    @Override
    public void orderedSearch(SearchRequest request, StreamObserver<SearchResponse> responseObserver) {
        throw new UnsupportedOperationException("orderedSearch not supported.");
    }

    @Override
    public void groupSearch(SearchRequest request, StreamObserver<SearchResponse> responseObserver) {

    }

    @Override
    public void delete(final Index request, final StreamObserver<ServiceResponse> responseObserver) {
        throw new UnsupportedOperationException("delete not supported.");
    }

    @Override
    public void getUpdateSequence(final Index request, final StreamObserver<UpdateSeq> responseObserver) {
        throw new UnsupportedOperationException("getUpdateSequence not supported.");
    }

    @Override
    public void info(final Index request, final StreamObserver<InfoResponse> responseObserver) {
        throw new UnsupportedOperationException("info not supported.");
    }

    @Override
    public void search(final SearchRequest request, final StreamObserver<SearchResponse> responseObserver) {
        final Query query = queryParser.parse(request.getQuery());
        final int limit = request.getLimit();
        final boolean staleOk = request.getStale();

        final SearchHandler handler = getOrOpen(request.getIndex());
        final SearchResponse response;

        if (request.hasGrouping()) {
            final String groupBy = request.getGrouping().getBy();
            final Sort groupSort = parseSort(request.getGrouping().getSort());
            final int groupOffset = request.getGrouping().getOffset();
            final int groupLimit = request.getGrouping().getLimit();

            final TopGroups<BytesRef> result = handler
                    .groupingSearch(query, groupBy, groupSort, groupOffset, groupLimit, limit, staleOk);
            return;
        } else if (request.hasSort()) {
            final Sort sort = parseSort(request.getSort());
            final TopFieldDocs topFieldDocs = handler.search(query, limit, sort, staleOk);
            response = SearchResponse.newBuilder().build();
        } else {
            final TopDocs topDocs = handler.search(query, limit, staleOk);
            response = SearchResponse.newBuilder().build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void setUpdateSequence(final SetUpdateSeq request, final StreamObserver<ServiceResponse> responseObserver) {
        throw new UnsupportedOperationException("setUpdateSequence not supported.");
    }

    @Override
    public StreamObserver<DocumentUpdate> update(final StreamObserver<ServiceResponse> responseObserver) {
        throw new UnsupportedOperationException("update not supported.");
    }

    private SearchHandler getOrOpen(final Index index) {
        final Subspace indexSubspace = new Subspace(index.getPrefix().toByteArray());
        final SearchHandler result = handlers.computeIfAbsent(indexSubspace, key -> {
            try {
                return SearchHandler.open(db, key, new StandardAnalyzer());
            } catch (final IOException e) {
                return null;
            }
        });
        if (result == null) {
            throw new IllegalArgumentException(index + " is not an index.");
        }
        return result;
    }

    private Sort parseSort(final com.cloudant.search3.grpc.Search3.SearchRequest.Sort sort) {
        final Sort result = new Sort();
        sort.getFieldsList().forEach(str -> {

        });
        return result;
    }

}
