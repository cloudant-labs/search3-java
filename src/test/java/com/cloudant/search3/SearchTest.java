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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.cloudant.search3.grpc.Search3.DocumentField;
import com.cloudant.search3.grpc.Search3.DocumentUpdate;
import com.cloudant.search3.grpc.Search3.FieldValue;
import com.cloudant.search3.grpc.Search3.Index;
import com.cloudant.search3.grpc.Search3.SearchRequest;
import com.cloudant.search3.grpc.Search3.SearchResponse;
import com.cloudant.search3.grpc.Search3.ServiceResponse;
import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;

public class SearchTest extends BaseFDBTest {

    private static class CollectingStreamObserver<T> implements StreamObserver<T> {

        private T lastValue;
        private Throwable lastThrowable;
        private boolean completed;

        @Override
        public void onNext(final T value) {
            this.lastValue = value;
        }

        @Override
        public void onError(final Throwable t) {
            this.lastThrowable = t;
        }

        @Override
        public void onCompleted() {
            this.completed = true;
        }

        @Override
        public String toString() {
            return String
                    .format("lastValue: %s, lastThrowable: %s, completed: %b\n", lastValue, lastThrowable, completed);
        }

    };

    @Test
    public void indexAndSearch() throws Exception {
        final Search search = new Search(DB);

        final Index index = Index.newBuilder().setPrefix(ByteString.copyFrom(prefix)).build();

        // Index something.
        final CollectingStreamObserver<ServiceResponse> updateCollector = new CollectingStreamObserver<ServiceResponse>();
        final StreamObserver<DocumentUpdate> stream = search.update(updateCollector);
        final DocumentUpdate docUpdate = DocumentUpdate.newBuilder().setIndex(index).setId("foobar")
                .addFields(field("foo", "bar baz", true)).build();
        stream.onNext(docUpdate);
        stream.onCompleted();
        assertNotNull(updateCollector.lastValue);
        assertNull(updateCollector.lastThrowable);
        assertTrue(updateCollector.completed);

        // Find it with a search?
        final SearchRequest searchRequest = SearchRequest.newBuilder().setIndex(index).setQuery("foo:bar").build();

        final CollectingStreamObserver<SearchResponse> searchResponseCollector = new CollectingStreamObserver<SearchResponse>();
        search.search(searchRequest, searchResponseCollector);

        final SearchResponse searchResponse = searchResponseCollector.lastValue;
        assertEquals(1, searchResponse.getMatches());
        assertEquals("foobar", searchResponse.getHits(0).getId());
    }

    private DocumentField field(final String name, final String value, final boolean analyzed) {
        return DocumentField.newBuilder().setName(name).setValue(fieldValue(value)).setAnalyzed(analyzed).build();
    }

    private FieldValue.Builder fieldValue(final double value) {
        return FieldValue.newBuilder().setDoubleValue(value);
    }

    private FieldValue.Builder fieldValue(final String value) {
        return FieldValue.newBuilder().setStringValue(value);
    }

}
