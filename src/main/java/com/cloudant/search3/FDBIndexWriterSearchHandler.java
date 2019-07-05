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

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.util.BytesRef;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.cloudant.fdblucene.FDBIndexReader;
import com.cloudant.fdblucene.FDBIndexWriter;
import com.cloudant.search3.grpc.Search3.InfoResponse;

public class FDBIndexWriterSearchHandler extends BaseSearchHandler {

    public static SearchHandlerFactory factory() {
        return new SearchHandlerFactory() {

            @Override
            public SearchHandler open(final Database db, final Subspace index, final Analyzer analyzer)
                    throws IOException {
                return new FDBIndexWriterSearchHandler(db, index, analyzer);
            }

        };
    }

    private final String toString;
    private final Database db;
    private final byte[] updateSeqKey;
    private final FDBIndexWriter writer;
    private final FDBIndexReader reader;
    private final IndexSearcher searcher;
    private String pendingUpdateSeq;

    private FDBIndexWriterSearchHandler(final Database db, final Subspace index, final Analyzer analyzer) {
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
    public void deleteDocument(final Term term) throws IOException {
        logger.info("deleteDocument({})", term);
        writer.deleteDocuments(term);
    }

    @Override
    public String getUpdateSeq() {
        return db.read(txn -> {
            return txn.get(updateSeqKey).thenApply(v -> {
                if (v == null) {
                    return null;
                }
                return Tuple.fromBytes(v).getString(0);
            });
        }).join();
    }

    @Override
    public TopGroups<BytesRef> groupingSearch(
            final Query query,
            final String groupBy,
            final Sort groupSort,
            final int groupOffset,
            final int groupLimit,
            final int groupDocsLimit,
            final boolean staleOk) throws IOException {
        throw new UnsupportedOperationException("groupingSearch not supported.");
    }

    @Override
    public InfoResponse info() throws IOException {
        return null;
    }

    @Override
    public void setPendingUpdateSeq(final String pendingUpdateSeq) {
        this.pendingUpdateSeq = pendingUpdateSeq;
    }

    @Override
    public void updateDocument(final Term term, final Document doc) throws IOException {
        logger.info("updateDocument({}, {})", term, doc);
        writer.updateDocument(term, doc);
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
