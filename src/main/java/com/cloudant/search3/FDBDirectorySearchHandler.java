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
import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.grouping.GroupingSearch;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.subspace.Subspace;
import com.cloudant.fdblucene.FDBDirectory;
import com.cloudant.search3.grpc.Search3.InfoResponse;

/**
 * Provides all services for a specific index using an FDBDirectory; should be
 * cached.
 */
public final class FDBDirectorySearchHandler extends BaseSearchHandler {

    public static SearchHandlerFactory factory() {
        return new SearchHandlerFactory() {

            @Override
            public SearchHandler open(final Database db, final Subspace index, final Analyzer analyzer)
                    throws IOException {
                final Directory dir = FDBDirectory.open(db, index, PAGE_SIZE, TXN_SIZE);
                dir.deleteFile("write.lock"); // HACK!!!
                final IndexWriterConfig indexWriterConfig = indexWriterConfig(analyzer);
                final IndexWriter writer = new IndexWriter(dir, indexWriterConfig);
                final SearcherManager manager = new SearcherManager(writer, null);
                return new FDBDirectorySearchHandler(writer, manager);
            }

        };
    }

    private static final double GROUP_CACHING_MB = 4.0;
    private static final int PAGE_SIZE = 10_000;
    private static final int TXN_SIZE = 10 * PAGE_SIZE;

    private final String toString;
    private final IndexWriter writer;
    private final SearcherManager manager;
    private boolean dirty = false;

    private FDBDirectorySearchHandler(final IndexWriter writer, final SearcherManager manager) {
        this.toString = String.format("FDBDirectorySearchHandler(%s)", writer.getDirectory());
        this.logger = LogManager.getLogger(writer.getDirectory().toString());
        this.writer = writer;
        this.manager = manager;
    }

    @Override
    public void close() throws IOException {
        this.manager.close();
        this.writer.rollback();
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

        final GroupingSearch groupingSearch = new GroupingSearch(groupBy);
        groupingSearch.setGroupSort(groupSort);
        groupingSearch.setCachingInMB(GROUP_CACHING_MB, true);
        groupingSearch.setAllGroups(false);
        groupingSearch.setGroupDocsLimit(groupDocsLimit);

        return withSearcher(staleOk, searcher -> {
            return groupingSearch.search(searcher, query, groupOffset, groupLimit);
        });
    }

    @Override
    public InfoResponse info() throws IOException {
        return null;
    }

    @Override
    public void updateDocument(final Term term, final Document doc) throws IOException {
        logger.info("updateDocument({}, {})", term, doc);
        this.writer.updateDocument(term, doc);
        this.dirty = true;
    }

    @Override
    public void deleteDocuments(final Term... terms) throws IOException {
        logger.info("deleteDocuments({})", Arrays.toString(terms));
        this.writer.deleteDocuments(terms);
        this.dirty = true;
    }

    @Override
    public void commit() throws IOException {
        if (!dirty) {
            return;
        }
        try {
            this.writer.commit();
            this.dirty = false;
            logger.info("committed.");
        } catch (final IOException e) {
            logger.catching(e);
        }
    }

    private static IndexWriterConfig indexWriterConfig(final Analyzer analyzer) {
        final IndexWriterConfig result = new IndexWriterConfig(analyzer);
        result.setUseCompoundFile(false);
        return result;
    }

    @Override
    protected <T> T withSearcher(final boolean staleOk, final IOFunction<IndexSearcher, T> f) throws IOException {
        if (!staleOk) {
            manager.maybeRefreshBlocking();
        }
        final IndexSearcher searcher = manager.acquire();
        try {
            return f.apply(searcher);
        } finally {
            manager.release(searcher);
        }
    }

    @Override
    public String toString() {
        return toString;
    }

}
