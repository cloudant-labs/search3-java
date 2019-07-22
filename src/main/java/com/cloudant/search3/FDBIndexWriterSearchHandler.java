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
import java.util.concurrent.CompletionException;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.cloudant.fdblucene.FDBIndexReader;
import com.cloudant.fdblucene.FDBIndexWriter;
import com.cloudant.search3.grpc.Search3.DocumentDeleteRequest;
import com.cloudant.search3.grpc.Search3.DocumentUpdateRequest;
import com.cloudant.search3.grpc.Search3.GroupSearchRequest;
import com.cloudant.search3.grpc.Search3.GroupSearchResponse;
import com.cloudant.search3.grpc.Search3.InfoResponse;
import com.cloudant.search3.grpc.Search3.UpdateSeq;

public final class FDBIndexWriterSearchHandler extends BaseSearchHandler {

    private final String toString;
    private final Database db;
    private final byte[] updateSeqKey;
    private final FDBIndexWriter writer;
    private final FDBIndexReader reader;
    private final IndexSearcher searcher;
    private UpdateSeq pendingUpdateSeq;

    FDBIndexWriterSearchHandler(final Database db, final Subspace index, final Analyzer analyzer) {
        super(analyzer);
        this.toString = String.format("FDBIndexWriterSearchHandler(%s)", index);
        this.logger = LogManager.getLogger(toString);
        this.db = db;
        this.updateSeqKey = index.pack("_update_seq");
        writer = new FDBIndexWriter(db, index, analyzer);
        reader = new FDBIndexReader(index);
        searcher = new IndexSearcher(reader);
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    @Override
    public void commit() throws IOException {
        if (pendingUpdateSeq == null) {
            return;
        }
        final byte[] value = Tuple.from(pendingUpdateSeq).pack();
        db.run(txn -> {
            txn.set(updateSeqKey, value);
            return null;
        });
        logger.info("committed at update sequence \"{}\".", pendingUpdateSeq);
        this.pendingUpdateSeq = null;
    }

    @Override
    public void deleteDocument(final DocumentDeleteRequest request) throws IOException {
        final String id = request.getId();
        if (id == null || id.isEmpty()) {
            throw new IllegalArgumentException("doc id is missing.");
        }

        final UpdateSeq seq = request.getSeq();
        if (seq == null || seq.getSeq() == null || seq.getSeq().isEmpty()) {
            throw new IllegalArgumentException("seq is missing.");
        }

        logger.info("deleteDocument({})", id);
        writer.deleteDocuments(new Term("_id", id));
        this.pendingUpdateSeq = seq;
    }

    @Override
    public GroupSearchResponse groupSearch(final GroupSearchRequest request) throws IOException {
        throw new UnsupportedOperationException("groupSearch not supported.");
    }

    @Override
    public InfoResponse info() throws IOException {
        final InfoResponse.Builder builder = InfoResponse.newBuilder();
        builder.setPurgeSeq("0");

        try {
        db.run(txn -> {
            final byte[] seqValue = txn.get(updateSeqKey).join();
            if (seqValue == null) {
                builder.setCommittedSeq("0");
            } else {
                builder.setCommittedSeq(Tuple.fromBytes(seqValue).getString(0));
            }

            try {
                reader.run(txn, () -> {
                    builder.setDocCount(reader.numDocs());
                    builder.setDocDelCount(reader.numDeletedDocs());
                    return null;
                });
            } catch (final IOException e) {
                throw new CompletionException(e);
            }
            return null;
            });
        } catch (final CompletionException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            throw e;
        }

        return builder.build();
    }

    @Override
    public void updateDocument(final DocumentUpdateRequest request) throws IOException {
        final String id = request.getId();
        if (id == null || id.isEmpty()) {
            throw new IllegalArgumentException("doc id is missing.");
        }

        final UpdateSeq seq = request.getSeq();
        if (seq == null || seq.getSeq() == null || seq.getSeq().isEmpty()) {
            throw new IllegalArgumentException("seq is missing.");
        }
        final Document doc = toDoc(request);
        logger.info("updateDocument({}, {})", id, doc);
        writer.updateDocument(new Term("_id", id), doc);
        this.pendingUpdateSeq = seq;
    }

    @Override
    protected <T> T withSearcher(final boolean staleOk, final IOFunction<IndexSearcher, T> f) throws IOException {
        return reader.run(db, () -> {
            return f.apply(searcher);
        });
    }

    @Override
    public String toString() {
        return toString;
    }

}
