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

import com.apple.foundationdb.Database;
import com.apple.foundationdb.subspace.Subspace;
import com.cloudant.fdblucene.FDBDirectory;
import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;

public final class FDBDirectorySearchHandlerFactory implements SearchHandlerFactory {

  private static final int PAGE_SIZE = 10_000;
  private static final int TXN_SIZE = 10 * PAGE_SIZE;

  private final Executor executor = Executors.newCachedThreadPool();

  private final SearcherFactory searcherFactory =
      new SearcherFactory() {

        @Override
        public IndexSearcher newSearcher(final IndexReader reader, final IndexReader previousReader)
            throws IOException {
          return new IndexSearcher(reader, executor);
        }
      };

  @Override
  public SearchHandler open(final Database db, final Subspace index, final Analyzer analyzer)
      throws IOException {
    final FDBDirectory dir = FDBDirectory.open(db, index, PAGE_SIZE, TXN_SIZE);
    forciblyUnlock(dir);
    final IndexWriterConfig indexWriterConfig = indexWriterConfig(analyzer);
    final IndexWriter writer = new IndexWriter(dir, indexWriterConfig);
    final SearcherManager manager = new SearcherManager(writer, searcherFactory);
    return new FDBDirectorySearchHandler(dir, writer, manager, analyzer);
  }

  /**
   * The current holder of the lock will know they lost the lock on their next attempt at a
   * destructive operation and will crash cleanly.
   */
  private void forciblyUnlock(final FDBDirectory dir) throws IOException {
    dir.unlock(IndexWriter.WRITE_LOCK_NAME);
  }

  private static IndexWriterConfig indexWriterConfig(final Analyzer analyzer) {
    final IndexWriterConfig result = new IndexWriterConfig(analyzer);
    result.setUseCompoundFile(false);
    result.setCommitOnClose(false);
    return result;
  }
}
