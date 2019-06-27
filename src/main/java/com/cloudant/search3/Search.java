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
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.util.BytesRef;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.subspace.Subspace;
import com.cloudant.search3.grpc.Search3.DocumentUpdate;
import com.cloudant.search3.grpc.Search3.FieldValue;
import com.cloudant.search3.grpc.Search3.Hit;
import com.cloudant.search3.grpc.Search3.Index;
import com.cloudant.search3.grpc.Search3.IndexableField;
import com.cloudant.search3.grpc.Search3.InfoResponse;
import com.cloudant.search3.grpc.Search3.SearchRequest;
import com.cloudant.search3.grpc.Search3.SearchResponse;
import com.cloudant.search3.grpc.Search3.SearchTerm;
import com.cloudant.search3.grpc.Search3.ServiceResponse;
import com.cloudant.search3.grpc.Search3.SetUpdateSeq;
import com.cloudant.search3.grpc.Search3.UpdateSeq;
import com.cloudant.search3.grpc.SearchGrpc;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

public final class Search extends SearchGrpc.SearchImplBase {

    private static final ServiceResponse OK = ServiceResponse.newBuilder().setCode(0).build();

    private final Database db;
    private final Analyzer analyzer = new StandardAnalyzer(); // TODO specify by index.
    private final QueryParser queryParser = new QueryParser("default", analyzer);
    private final ConcurrentMap<Subspace, SearchHandler> handlers = new ConcurrentHashMap<Subspace, SearchHandler>();

    public Search(final Database db) {
        this.db = db;
    }

    @Override
    public void delete(final Index request, final StreamObserver<ServiceResponse> responseObserver) {
        final SearchHandler handler = getOrOpen(request);
        try {
            handler.close();
        } catch (IOException e) {
            responseObserver.onError(Status.UNKNOWN.asException());
        }
        db.run(txn -> {
            txn.clear(toSubspace(request).range());
            return null;
        });
        responseObserver.onNext(OK);
        responseObserver.onCompleted();
    }

    @Override
    public void getUpdateSequence(final Index request, final StreamObserver<UpdateSeq> responseObserver) {
        final SearchHandler handler = getOrOpen(request);
        final UpdateSeq updateSeq = UpdateSeq.newBuilder().setSeq(handler.getUpdateSeq()).build();
        responseObserver.onNext(updateSeq);
        responseObserver.onCompleted();
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
            final boolean staleOk = request.getStale();

            final SearchHandler handler = getOrOpen(request.getIndex());
            final SearchResponse response;

            if (request.hasGrouping()) {
                response = groupSearch(request, query, limit, staleOk, handler);
            } else if (request.hasSort()) {
                response = sortedSearch(request, query, limit, staleOk, handler);
            } else {
                response = relevanceSearch(query, limit, staleOk, handler);
            }
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (final IOException e) {
            responseObserver.onError(Status.UNKNOWN.asException());
            return;
        } catch (final ParseException e) {
            responseObserver.onError(Status.UNKNOWN.asException());
            return;
        }

    }

    private SearchResponse groupSearch(
            final SearchRequest request,
            final Query query,
            final int limit,
            final boolean staleOk,
            final SearchHandler handler) throws IOException {
        final SearchResponse response;
        final String groupBy = request.getGrouping().getBy();
        final Sort groupSort = toSort(request.getGrouping().getSort());
        final int groupOffset = request.getGrouping().getOffset();
        final int groupLimit = request.getGrouping().getLimit();

        final TopGroups<BytesRef> result = handler
                .groupingSearch(query, groupBy, groupSort, groupOffset, groupLimit, limit, staleOk);
        response = SearchResponse.newBuilder().build();
        return response;
    }

    private SearchResponse sortedSearch(
            final SearchRequest request,
            final Query query,
            final int limit,
            final boolean staleOk,
            final SearchHandler handler) throws IOException {
        final Sort sort = toSort(request.getSort());
        final TopFieldDocs topFieldDocs = handler.search(query, limit, sort, staleOk);
        final SearchResponse.Builder builder = SearchResponse.newBuilder();
        builder.setMatches(topFieldDocs.totalHits.value);
        for (int i = 0; i < topFieldDocs.scoreDocs.length; i++) {

        }
        return builder.build();
    }

    private SearchResponse relevanceSearch(
            final Query query,
            final int limit,
            final boolean staleOk,
            final SearchHandler handler) throws IOException {
        final TopDocs topDocs = handler.search(query, limit, staleOk);
        final SearchResponse.Builder builder = SearchResponse.newBuilder();
        builder.setMatches(topDocs.totalHits.value);
        for (final ScoreDoc scoreDoc : topDocs.scoreDocs) {
            builder.addHits(toHit(scoreDoc));
        }
        return builder.build();
    }

    @Override
    public void setUpdateSequence(final SetUpdateSeq request, final StreamObserver<ServiceResponse> responseObserver) {
        final SearchHandler handler = getOrOpen(request.getIndex());
        handler.setPendingUpdateSeq(request.getSeq());
        responseObserver.onNext(OK);
        responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<DocumentUpdate> update(final StreamObserver<ServiceResponse> responseObserver) {
        return new StreamObserver<DocumentUpdate>() {

            @Override
            public void onNext(final DocumentUpdate request) {
                final SearchHandler handler = getOrOpen(request.getIndex());
                final Term term = toTerm(request.getTerm());

                try {
                    if (request.getFieldsCount() == 0) {
                        handler.deleteDocuments(term);
                    } else {
                        handler.updateDocument(term, toDoc(request.getFieldsList()));
                    }
                } catch (final IOException e) {
                    responseObserver.onError(Status.UNKNOWN.asException());
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

    private SearchHandler getOrOpen(final Index index) {
        final Subspace indexSubspace = toSubspace(index);
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

    private Subspace toSubspace(final Index index) {
        return new Subspace(index.getPrefix().toByteArray());
    }

    private Sort toSort(final com.cloudant.search3.grpc.Search3.SearchRequest.Sort sort) {
        final Sort result = new Sort();
        sort.getFieldsList().forEach(str -> {

        });
        return result;
    }

    private Term toTerm(final SearchTerm searchTerm) {
        return new Term(searchTerm.getField(), searchTerm.getValue());
    }

    private Document toDoc(final List<IndexableField> fields) throws IOException {
        final DocumentBuilder builder = new DocumentBuilder();

        for (final IndexableField field : fields) {
            final String name = field.getName();
            final FieldValue value = field.getValue();
            final boolean analyzed = field.getAnalyzed();
            final boolean stored = field.getStored();
            final boolean facet = field.getFacet();

            switch (value.getValueOneofCase()) {
            case BOOL_VALUE:
                builder.addBoolean(name, value.getBoolValue(), stored);
                break;
            case DOUBLE_VALUE:
                builder.addDouble(name, value.getDoubleValue(), stored);
                break;
            case STRING_VALUE:
                if (analyzed) {
                    builder.addText(name, value.getStringValue(), stored, facet);
                } else {
                    builder.addString(name, value.getStringValue(), stored);
                }
                break;
            default:
                // Ignore field with no value.
                break;
            }
        }
        return builder.build();
    }

    private Hit toHit(final ScoreDoc scoreDoc) {
        final Hit.Builder builder = Hit.newBuilder();
        builder.setOrder(0, FieldValue.newBuilder().setDoubleValue(scoreDoc.score));
        return builder.build();
    }

    private Query toQuery(final SearchRequest request) throws ParseException {
        final Query query = queryParser.parse(request.getQuery());
        final String partition = request.getPartition();
        if (partition.length() > 0) {
            return addPartition(query, partition);
        } else {
            return query;
        }
    }

    private Query addPartition(final Query query, final String partition) {
        final BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(new TermQuery(new Term("_partition", partition)), Occur.MUST);
        builder.add(query, Occur.MUST);
        return builder.build();
    }

}
