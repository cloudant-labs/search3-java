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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.cloudant.search3.grpc.Search3.DocumentField;
import com.cloudant.search3.grpc.Search3.DocumentUpdate;
import com.cloudant.search3.grpc.Search3.FieldValue;
import com.cloudant.search3.grpc.Search3.Group;
import com.cloudant.search3.grpc.Search3.GroupSearchRequest;
import com.cloudant.search3.grpc.Search3.GroupSearchResponse;
import com.cloudant.search3.grpc.Search3.Hit;
import com.cloudant.search3.grpc.Search3.Index;
import com.cloudant.search3.grpc.Search3.SearchRequest;
import com.cloudant.search3.grpc.Search3.SearchResponse;
import com.cloudant.search3.grpc.Search3.SetUpdateSeq;
import com.cloudant.search3.grpc.Search3.UpdateSeq;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;

import io.grpc.stub.StreamObserver;

@RunWith(Parameterized.class)
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

    };

    @Parameters
    public static Collection<SearchHandlerFactory> factories() {
        return Arrays.asList(
                new SearchHandlerFactory[] { FDBIndexWriterSearchHandler.factory(),
                        FDBDirectorySearchHandler.factory() });
    }

    private final SearchHandlerFactory factory;

    public SearchTest(final SearchHandlerFactory factory) {
        this.factory = factory;
    }

    @Test
    public void indexAndSearch() throws Exception {
        try (final Search search = new Search(DB, factory)) {

            final Index index = Index.newBuilder().setPrefix(ByteString.copyFrom(prefix)).build();

            // Index something.
            final CollectingStreamObserver<Empty> serviceResponseCollector = new CollectingStreamObserver<Empty>();
            final DocumentUpdate docUpdate = DocumentUpdate.newBuilder().setIndex(index).setId("foobar")
                    .addFields(field("foo", "bar baz", true)).build();
            search.updateDocument(docUpdate, serviceResponseCollector);
            assertNotNull(serviceResponseCollector.lastValue);
            assertNull(serviceResponseCollector.lastThrowable);
            assertTrue(serviceResponseCollector.completed);

            // Find it with a search?
            final SearchRequest searchRequest = SearchRequest.newBuilder().setIndex(index).setQuery("foo:bar").build();

            final CollectingStreamObserver<SearchResponse> searchResponseCollector = new CollectingStreamObserver<SearchResponse>();
            search.search(searchRequest, searchResponseCollector);

            final SearchResponse searchResponse = searchResponseCollector.lastValue;
            assertEquals(1, searchResponse.getMatches());
            assertEquals("foobar", searchResponse.getHits(0).getId());
        }
    }

    @Test
    public void indexAndGroupSearch() throws Exception {
        try (final Search search = new Search(DB, factory)) {

            final Index index = Index.newBuilder().setPrefix(ByteString.copyFrom(prefix)).build();

            {
                // Index something.
                final CollectingStreamObserver<Empty> collector = new CollectingStreamObserver<Empty>();
                final DocumentUpdate docUpdate = DocumentUpdate.newBuilder().setIndex(index).setId("foobar")
                        .addFields(field("foo", "bar baz", true)).addFields(field("category", "foobar", false)).build();
                search.updateDocument(docUpdate, collector);
                assertNotNull(collector.lastValue);
                assertNull(collector.lastThrowable);
                assertTrue(collector.completed);
            }

            {
                // Find it with a search?
                final GroupSearchRequest groupSearchRequest = GroupSearchRequest.newBuilder().setIndex(index)
                        .setGroupBy("category").setQuery("foo:bar").build();

                final CollectingStreamObserver<GroupSearchResponse> collector = new CollectingStreamObserver<GroupSearchResponse>();
                search.groupSearch(groupSearchRequest, collector);

                final GroupSearchResponse searchResponse = collector.lastValue;
                assertEquals(1, searchResponse.getMatches());
                assertEquals(1, searchResponse.getGroupMatches());
                assertEquals(1, searchResponse.getGroupsCount());

                final Group group = searchResponse.getGroups(0);
                assertEquals("foobar", group.getBy());
                assertEquals(1, group.getMatches());
                assertEquals(1, group.getHitsCount());

                final Hit hit = group.getHits(0);
                assertEquals("foobar", hit.getId());
            }
        }
    }

    @Test
    public void getSetUpdateSeq() throws Exception {
        try (final Search search = new Search(DB, factory)) {
            final Index index = Index.newBuilder().setPrefix(ByteString.copyFrom(prefix)).build();

            {
                final CollectingStreamObserver<UpdateSeq> collector = new CollectingStreamObserver<UpdateSeq>();
                search.getUpdateSequence(index, collector);
                assertNull(collector.lastValue);
                assertNull(collector.lastThrowable);
            }

            {
                final CollectingStreamObserver<Empty> collector = new CollectingStreamObserver<Empty>();
                search.setUpdateSequence(SetUpdateSeq.newBuilder().setIndex(index).setSeq("foo").build(), collector);
                assertSame(Empty.getDefaultInstance(), collector.lastValue);
            }

            // Wait for the commit :/.
            Thread.sleep(5500);

            {
                final CollectingStreamObserver<UpdateSeq> collector = new CollectingStreamObserver<UpdateSeq>();
                search.getUpdateSequence(index, collector);
                assertEquals(UpdateSeq.newBuilder().setSeq("foo").build(), collector.lastValue);
            }
        }
    }

    private DocumentField field(final String name, final String value, final boolean analyzed) {
        return DocumentField.newBuilder().setName(name).setValue(fieldValue(value)).setAnalyzed(analyzed)
                .setStored(true).build();
    }

    private FieldValue.Builder fieldValue(final double value) {
        return FieldValue.newBuilder().setDouble(value);
    }

    private FieldValue.Builder fieldValue(final String value) {
        return FieldValue.newBuilder().setString(value);
    }

}
