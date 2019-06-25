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

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.util.BytesRef;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.subspace.Subspace;

public final class Search3Server {

    private final Database db;
    private final ConcurrentMap<Subspace, SearchHandler> handlers = new ConcurrentHashMap<Subspace, SearchHandler>();

    public Search3Server(final Database db) {
        this.db = db;
    }

    public void delete(final Subspace index) throws IOException {
        final SearchHandler handler = handlers.remove(index);
        handler.close();
        db.run(txn -> {
            txn.clear(index.range());
            return null;
        });
    }

    public void setUpdateSeq(final Subspace index) {
        final SearchHandler handler = getOrOpen(index);
        handler.setUpdateSeq(index);
    }

    public void info(final Subspace index) {
        // TODO
    }

    public void setUpdateSeq(final Subspace index, final String updateSeq) throws IOException {
        final SearchHandler handler = getOrOpen(index);
        handler.setUpdateSeq(updateSeq);
    }

    public TopDocs search(final Subspace index, final Query query, final int n, final boolean staleOk)
            throws IOException {
        final SearchHandler handler = getOrOpen(index);
        return handler.search(query, n, staleOk);
    }

    public TopFieldDocs search(
            final Subspace index,
            final Query query,
            final int n,
            final Sort sort,
            final boolean staleOk) throws IOException {
        final SearchHandler handler = getOrOpen(index);
        return handler.search(query, n, sort, staleOk);
    }

    public TopGroups<BytesRef> search(
            final Subspace index,
            final Query query,
            final String groupBy,
            final Sort groupSort,
            final int groupOffset,
            final int groupLimit,
            final int groupDocsLimit,
            final boolean staleOk) throws IOException {
        final SearchHandler handler = getOrOpen(index);
        return handler.groupingSearch(query, groupBy, groupSort, groupOffset, groupLimit, groupDocsLimit, staleOk);
    }

    public void update(final Subspace index, final Term term, final Document doc) throws IOException {
        final SearchHandler handler = getOrOpen(index);
        handler.updateDocument(term, doc);
    }

    private SearchHandler getOrOpen(final Subspace index) {
        final SearchHandler result = handlers.computeIfAbsent(index, key -> {
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

}
