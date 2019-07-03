// Copyright 2019 IBM
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudant.search3;

import java.io.IOException;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.util.BytesRef;

import com.cloudant.search3.grpc.Search3.InfoResponse;
import com.cloudant.search3.grpc.Search3.SearchResponse;

public interface SearchHandler {

    void close() throws IOException;

    void commit() throws IOException;

    void deleteDocuments(Term... terms) throws IOException;

    TopGroups<BytesRef> groupingSearch(
            Query query,
            String groupBy,
            Sort groupSort,
            int groupOffset,
            int groupLimit,
            int groupDocsLimit,
            boolean staleOk) throws IOException;

    InfoResponse info() throws IOException;

    SearchResponse search(Query query, int n, Set<String> fieldsToLoad, boolean staleOk) throws IOException;

    SearchResponse search(Query query, int n, Sort sort, Set<String> fieldsToLoad, boolean staleOk) throws IOException;

    void updateDocument(Term term, Document doc) throws IOException;
}