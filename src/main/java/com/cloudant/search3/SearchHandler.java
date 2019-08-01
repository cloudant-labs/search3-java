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

import org.apache.lucene.queryparser.classic.ParseException;

import com.cloudant.search3.grpc.Search3.DocumentDeleteRequest;
import com.cloudant.search3.grpc.Search3.DocumentUpdateRequest;
import com.cloudant.search3.grpc.Search3.GroupSearchRequest;
import com.cloudant.search3.grpc.Search3.GroupSearchResponse;
import com.cloudant.search3.grpc.Search3.Index;
import com.cloudant.search3.grpc.Search3.InfoResponse;
import com.cloudant.search3.grpc.Search3.SearchRequest;
import com.cloudant.search3.grpc.Search3.SearchResponse;
import com.cloudant.search3.grpc.Search3.SessionResponse;
import com.cloudant.search3.grpc.Search3.SetUpdateSeqRequest;

public interface SearchHandler {
    void close() throws IOException;

    void commit() throws IOException;

    SessionResponse deleteDocument(final DocumentDeleteRequest request) throws IOException;

    GroupSearchResponse groupSearch(GroupSearchRequest request) throws IOException, ParseException;

    InfoResponse info(final Index index) throws IOException;

    SearchResponse search(final SearchRequest request) throws IOException, ParseException;

    SessionResponse setUpdateSeq(final SetUpdateSeqRequest request) throws IOException;

    SessionResponse updateDocument(final DocumentUpdateRequest request) throws IOException;
}
