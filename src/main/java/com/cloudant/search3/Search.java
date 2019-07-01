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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.util.BytesRef;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.subspace.Subspace;
import com.cloudant.search3.grpc.Search3.DocumentField;
import com.cloudant.search3.grpc.Search3.DocumentUpdate;
import com.cloudant.search3.grpc.Search3.FieldValue;
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

    private static final SortField INVERSE_FIELD_SCORE = new SortField(null, SortField.Type.SCORE, true);
    private static final SortField INVERSE_FIELD_DOC = new SortField(null, SortField.Type.DOC, true);

    private static final Pattern SORT_FIELD_RE = Pattern.compile("^([-+])?([\\.\\w]+)(?:<(\\w+)>)?$");
    private static final String FP = "([-+]?[0-9]+(?:\\.[0-9]+)?)";
    private static final Pattern DISTANCE_RE = Pattern
            .compile("^([-+])?<distance,([\\.\\w]+),([\\.\\w]+),%s,%s,(mi|km)>$".format(FP, FP));

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
            responseObserver.onError(Status.UNKNOWN.asException());
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
            responseObserver.onError(Status.ABORTED.asException());
        }
    }

    @Override
    public void info(final Index request, final StreamObserver<InfoResponse> responseObserver) {
        throw new UnsupportedOperationException("info not supported.");
    }

    @Override
    public void search(final SearchRequest request, final StreamObserver<SearchResponse> responseObserver) {
        try {
            final Query query = toQuery(request.getQuery(), request.getPartition());
            final int limit = request.getLimit();
            final Set<String> fieldsToLoad = new HashSet<String>(request.getIncludeFieldsList());
            final boolean staleOk = request.getStale();

            final SearchHandler handler = getOrOpen(request.getIndex());

            final SearchResponse response;
            if (request.hasSort()) {
                final Sort sort = toSort(request.getSort());
                response = handler.search(query, limit, sort, fieldsToLoad, staleOk);
            } else {
                response = handler.search(query, limit, fieldsToLoad, staleOk);
            }
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (final IOException e) {
            responseObserver.onError(Status.ABORTED.asException());
        } catch (final ParseException e) {
            responseObserver.onError(Status.ABORTED.asException());
        }
    }

    @Override
    public void groupSearch(
            final GroupSearchRequest request,
            final StreamObserver<GroupSearchResponse> responseObserver) {
        try {
            final Query query = toQuery(request.getQuery(), "");
            final int limit = request.getLimit();
            final boolean staleOk = request.getStale();
            final String groupBy = request.getGroupBy();
            final Sort groupSort = toSort(request.getGroupSort());
            final int groupOffset = request.getGroupOffset();
            final int groupLimit = request.getGroupLimit();

            final SearchHandler handler = getOrOpen(request.getIndex());
            final TopGroups<BytesRef> result = handler
                    .groupingSearch(query, groupBy, groupSort, groupOffset, groupLimit, limit, staleOk);

            final GroupSearchResponse response = GroupSearchResponse.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (final IOException e) {
            responseObserver.onError(Status.ABORTED.asException());
        } catch (final ParseException e) {
            responseObserver.onError(Status.ABORTED.asException());
        }
    }

    @Override
    public void setUpdateSequence(final SetUpdateSeq request, final StreamObserver<ServiceResponse> responseObserver) {
        try {
            final SearchHandler handler = getOrOpen(request.getIndex());
            handler.setPendingUpdateSeq(request.getSeq());
        } catch (final IOException e) {
            responseObserver.onError(Status.ABORTED.asException());
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
                        final Document doc = toDoc(request.getId(), request.getFieldsList());
                        handler.updateDocument(idTerm, doc);
                    }
                } catch (final IOException e) {
                    responseObserver.onError(Status.ABORTED.asException());
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
                return SearchHandler.open(db, key, new StandardAnalyzer());
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

    private Sort toSort(final com.cloudant.search3.grpc.Search3.Sort sort) throws ParseException {
        final SortField[] sortFields = new SortField[sort.getFieldsCount()];
        for (int i = 0; i < sort.getFieldsCount(); i++) {
            switch (sort.getFields(i)) {
            case "<score>":
                sortFields[i] = INVERSE_FIELD_SCORE;
                continue;
            case "-<score>":
                sortFields[i] = SortField.FIELD_SCORE;
                continue;
            case "<doc>":
                sortFields[i] = SortField.FIELD_DOC;
                continue;
            case "-<doc>":
                sortFields[i] = INVERSE_FIELD_DOC;
                continue;
            }

            Matcher m = DISTANCE_RE.matcher(sort.getFields(i));
            if (m.matches()) {
                throw new ParseException("sort by distance not yet supported.");
            }

            m = SORT_FIELD_RE.matcher(sort.getFields(i));
            if (m.matches()) {
                final String fieldTypeStr = m.group(3) == null ? "number" : m.group(3);
                final SortField.Type fieldType;
                switch (fieldTypeStr) {
                case "string":
                    fieldType = SortField.Type.STRING;
                    break;
                case "number":
                    fieldType = SortField.Type.DOUBLE;
                    break;
                default:
                    throw new ParseException("Unrecognized type: " + m.group(3));
                }
                final boolean reverse = "-".equals(m.group(1));

                sortFields[i] = new SortField(m.group(2), fieldType, reverse);
            }
        }
        return new Sort(sortFields);
    }

    private Document toDoc(final String id, final List<DocumentField> fields) throws IOException {
        final DocumentBuilder builder = new DocumentBuilder();
        builder.addString("_id", id, true);

        for (final DocumentField field : fields) {
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

    /**
     * Does "partition" even make sense for couch-on-fdb?
     */
    private Query toQuery(final String queryString, final String partition) throws ParseException {
        final Query baseQuery = queryParser.parse(queryString);
        if (partition.length() > 0) {
            final BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(new TermQuery(new Term("_partition", partition)), Occur.MUST);
            builder.add(baseQuery, Occur.MUST);
            return builder.build();
        } else {
            return baseQuery;
        }
    }

}
