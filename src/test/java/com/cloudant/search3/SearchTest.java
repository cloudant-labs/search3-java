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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.cloudant.search3.grpc.Search3.DocumentField;
import com.cloudant.search3.grpc.Search3.DocumentUpdateRequest;
import com.cloudant.search3.grpc.Search3.FieldValue;
import com.cloudant.search3.grpc.Search3.Group;
import com.cloudant.search3.grpc.Search3.GroupSearchRequest;
import com.cloudant.search3.grpc.Search3.GroupSearchResponse;
import com.cloudant.search3.grpc.Search3.Hit;
import com.cloudant.search3.grpc.Search3.Index;
import com.cloudant.search3.grpc.Search3.Path;
import com.cloudant.search3.grpc.Search3.Ranges;
import com.cloudant.search3.grpc.Search3.SearchRequest;
import com.cloudant.search3.grpc.Search3.SearchResponse;
import com.cloudant.search3.grpc.Search3.Sort;
import com.cloudant.search3.grpc.Search3.UpdateSeq;
import com.google.protobuf.ByteString;

@RunWith(Parameterized.class)
public class SearchTest extends BaseFDBTest {

    @Parameters
    public static Collection<SearchHandlerFactory> factories() {
        return Arrays.asList(
                new SearchHandlerFactory[] { /* new FDBIndexWriterSearchHandlerFactory(), */
                        new FDBDirectorySearchHandlerFactory() });
    }

    private final SearchHandlerFactory factory;
    private Configuration config;
    private int seq = 1;

    public SearchTest(final SearchHandlerFactory factory) {
        this.factory = factory;
    }

    @Before
    public void setup() throws Exception {
        super.setup();
        final Configurations configs = new Configurations();
        this.config = configs.ini(new File("search3.ini"));
        config.setProperty("handler_factory", factory.getClass().getCanonicalName());
        config.setProperty("commit_interval_secs", "1");
    }

    @Test
    public void basicSearch() throws Exception {
        try (final Search search = new Search(config)) {
            final Index index = Index.newBuilder().setPrefix(ByteString.copyFrom(prefix)).build();

            // Index something.
            search.updateDocument(update(index, "foobar", "foo", "bar baz", true, false));

            // Find it with a search?
            final SearchRequest searchRequest = SearchRequest.newBuilder().setIndex(index).setQuery("foo:bar")
                    .setLimit(25).build();

            final SearchResponse searchResponse = search.search(searchRequest);

            assertEquals(1, searchResponse.getMatches());
            assertEquals("foobar", searchResponse.getHits(0).getId());
        }
    }

    @Test
    public void countFacetSearch() throws Exception {
        try (final Search search = new Search(config)) {
            final Index index = Index.newBuilder().setPrefix(ByteString.copyFrom(prefix)).build();

            // Index something.
            search.updateDocument(update(index, "doc1", "foo", "bar", false, true));
            search.updateDocument(update(index, "doc2", "foo", "bar", false, true));

            // Find it with a search?
            final SearchRequest searchRequest = SearchRequest.newBuilder().setIndex(index).setQuery("_id:d*")
                    .addCounts("foo").setLimit(25).build();

            final SearchResponse searchResponse = search.search(searchRequest);
            assertEquals(2, searchResponse.getMatches());
            assertEquals("doc1", searchResponse.getHits(0).getId());
            assertTrue(searchResponse.containsCounts("foo"));
            assertEquals(2, searchResponse.getCountsOrThrow("foo").getCountsOrDefault("bar", 0));
        }
    }

    @Test
    public void rangeFacetSearch() throws Exception {
        try (final Search search = new Search(config)) {
            final Index index = Index.newBuilder().setPrefix(ByteString.copyFrom(prefix)).build();

            // Index something.
            search.updateDocument(update(index, "doc1", "foo", 1.0, false, true));
            search.updateDocument(update(index, "doc2", "foo", 2.0, false, true));

            final Ranges ranges = Ranges.newBuilder().putRanges("all", "[1 TO 2]").build();

            // Find it with a search?
            final SearchRequest searchRequest = SearchRequest.newBuilder().setIndex(index).setQuery("_id:d*")
                    .putRanges("foo", ranges).setLimit(25).build();

            final SearchResponse searchResponse = search.search(searchRequest);
            assertEquals(2, searchResponse.getMatches());
            assertEquals("doc1", searchResponse.getHits(0).getId());
            assertTrue(searchResponse.containsRanges("foo"));
            assertEquals(2, searchResponse.getRangesOrThrow("foo").getCountsOrDefault("all", 0));
        }
    }

    @Test
    public void drilldownSearch() throws Exception {
        try (final Search search = new Search(config)) {
            final Index index = Index.newBuilder().setPrefix(ByteString.copyFrom(prefix)).build();

            // Index something.
            search.updateDocument(update(index, "doc1", "foo", "bar", false, true));
            search.updateDocument(update(index, "doc2", "foo", "baz", false, true));

            final Path path = Path.newBuilder().addParts("foo").addParts("baz").build();

            // Find it with a search?
            final SearchRequest searchRequest = SearchRequest.newBuilder().setIndex(index).setQuery("_id:d*")
                    .addDrilldown(path)
                    .setLimit(25).build();

            final SearchResponse searchResponse = search.search(searchRequest);
            assertEquals(1, searchResponse.getMatches());
            assertEquals("doc2", searchResponse.getHits(0).getId());
        }
    }

    @Test
    public void sortByDistanceSearch() throws Exception {
        try (final Search search = new Search(config)) {
            final Index index = Index.newBuilder().setPrefix(ByteString.copyFrom(prefix)).build();

            // Index something.
            search.updateDocument(update(index, "foo", field("lon", 0.5, true, false), field("lat", 57.15, true, false)));
            search.updateDocument(update(index, "bar", field("lon", 10.0, true, false), field("lat", 57.15, true, false)));
            search.updateDocument(update(index, "zzz", field("lon", 3.0, true, false), field("lat", 57.15, true, false)));

            {
                final Sort sort = Sort.newBuilder().addFields("<distance,lon,lat,0.2,57.15,km>").build();
                final SearchRequest searchRequest = SearchRequest.newBuilder().setIndex(index).setQuery("*:*")
                        .setSort(sort).setLimit(25).build();

                final SearchResponse searchResponse = search.search(searchRequest);
                assertEquals(3, searchResponse.getMatches());
                assertEquals("foo", searchResponse.getHits(0).getId());
                assertEquals("zzz", searchResponse.getHits(1).getId());
                assertEquals("bar", searchResponse.getHits(2).getId());
            }

            {
                final Sort sort = Sort.newBuilder().addFields("<distance,lon,lat,12,57.15,km>").build();
                final SearchRequest searchRequest = SearchRequest.newBuilder().setIndex(index).setQuery("*:*")
                        .setSort(sort).setLimit(25).build();

                final SearchResponse searchResponse = search.search(searchRequest);
                assertEquals(3, searchResponse.getMatches());
                assertEquals("bar", searchResponse.getHits(0).getId());
                assertEquals("zzz", searchResponse.getHits(1).getId());
                assertEquals("foo", searchResponse.getHits(2).getId());
            }
        }
    }

    @Test
    public void sortSearch() throws Exception {
        try (final Search search = new Search(config)) {
            final Index index = Index.newBuilder().setPrefix(ByteString.copyFrom(prefix)).build();

            // Index something.
            search.updateDocument(update(index, "foobar", "foo", "bar baz", true, false));

            // Find it with a search?
            final Sort sort = Sort.newBuilder().addFields("foobar").build();
            final SearchRequest searchRequest = SearchRequest.newBuilder().setIndex(index).setQuery("foo:bar")
                    .setSort(sort).setLimit(25).build();

            final SearchResponse searchResponse = search.search(searchRequest);

            assertEquals(1, searchResponse.getMatches());
            assertEquals("foobar", searchResponse.getHits(0).getId());
        }
    }

    @Test
    public void indexAndGroupSearch() throws Exception {
        try (final Search search = new Search(config)) {

            final Index index = Index.newBuilder().setPrefix(ByteString.copyFrom(prefix)).build();
            {
                // Index something.
                search.updateDocument(update(index, "foobar", "category", "foobar", false, false));

                // Find it with a search?
                final GroupSearchRequest groupSearchRequest = GroupSearchRequest.newBuilder().setIndex(index)
                        .setGroupBy("category").setQuery("category:foobar").setLimit(25).setGroupLimit(25).build();

                final GroupSearchResponse searchResponse = search.groupSearch(groupSearchRequest);

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
    public void searchWithHighlights() throws Exception {
        try (final Search search = new Search(config)) {
            final Index index = Index.newBuilder().setPrefix(ByteString.copyFrom(prefix)).build();

            // Index something.
            search.updateDocument(update(index, "foobar", "foo", "bar baz", true, false));

            // Find it with a search?
            final SearchRequest searchRequest = SearchRequest.newBuilder().setIndex(index).setQuery("foo:bar")
                    .setLimit(25).addHighlightFields("foo").build();

            final SearchResponse searchResponse = search.search(searchRequest);

            assertEquals(1, searchResponse.getMatches());
            assertEquals("foo", searchResponse.getHits(0).getHighlights(0).getFieldname());
            assertEquals("<em>bar</em> baz", searchResponse.getHits(0).getHighlights(0).getHighlights(0));
        }
    }

    private DocumentUpdateRequest update(
            final Index index,
            final String id,
            final String name,
            final Object value,
            final boolean analyzed,
            final boolean facet) {
        return update(index, id, field(name, value, analyzed, facet));
    }

    private DocumentUpdateRequest update(final Index index, final String id, final DocumentField... fields) {
        final DocumentUpdateRequest.Builder builder = DocumentUpdateRequest.newBuilder();
        builder.setIndex(index);
        builder.setId(id);
        builder.setSeq(nextSeq());
        for (final DocumentField field : fields) {
            builder.addFields(field);
        }
        return builder.build();
    }

    private DocumentField field(final String name, final Object value, final boolean analyzed, final boolean facet) {
        return DocumentField.newBuilder().setName(name).setValue(fieldValue(value)).setAnalyzed(analyzed)
                .setStore(true).setFacet(facet).build();
    }

    private FieldValue.Builder fieldValue(final Object value) {
        if (value instanceof String) {
            return FieldValue.newBuilder().setString((String) value);
        }
        if (value instanceof Double) {
            return FieldValue.newBuilder().setDouble((double) value);
        }
        throw new IllegalArgumentException(value + " not recognised as field value.");
    }

    private UpdateSeq nextSeq() {
        return UpdateSeq.newBuilder().setSeq(Integer.toString(seq++)).build();
    }

}
