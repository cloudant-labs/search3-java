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

import com.cloudant.search3.grpc.Search3;
import com.cloudant.search3.grpc.Search3.Bookmark;
import com.cloudant.search3.grpc.Search3.Counts;
import com.cloudant.search3.grpc.Search3.DocumentField;
import com.cloudant.search3.grpc.Search3.DocumentUpdateRequest;
import com.cloudant.search3.grpc.Search3.FieldValue;
import com.cloudant.search3.grpc.Search3.GroupSearchRequest;
import com.cloudant.search3.grpc.Search3.Highlights;
import com.cloudant.search3.grpc.Search3.Hit;
import com.cloudant.search3.grpc.Search3.HitField;
import com.cloudant.search3.grpc.Search3.Index;
import com.cloudant.search3.grpc.Search3.Path;
import com.cloudant.search3.grpc.Search3.Ranges;
import com.cloudant.search3.grpc.Search3.SearchRequest;
import com.cloudant.search3.grpc.Search3.SearchResponse;
import com.cloudant.search3.grpc.Search3.SessionResponse;
import com.cloudant.search3.grpc.Search3.UpdateSeq;
import io.prometheus.client.Histogram;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollectorManager;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.range.DoubleRange;
import org.apache.lucene.facet.range.DoubleRangeFacetCounts;
import org.apache.lucene.facet.sortedset.DefaultSortedSetDocValuesReaderState;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetCounts;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiCollectorManager;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;
import org.apache.lucene.search.highlight.Formatter;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleFragmenter;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.util.BytesRef;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.impl.PointImpl;

public abstract class BaseSearchHandler implements SearchHandler {

  private static final String[] EMPTY_STRING_ARR = new String[0];
  private static final FieldValue NULL_VALUE = FieldValue.newBuilder().setNull(true).build();
  private static final ScoreDoc[] EMPTY_SCORE_DOC = new ScoreDoc[0];
  private static final SortField INVERSE_FIELD_SCORE =
      new SortField(null, SortField.Type.SCORE, true);
  private static final SortField INVERSE_FIELD_DOC = new SortField(null, SortField.Type.DOC, true);

  protected static final Pattern SORT_FIELD_RE =
      Pattern.compile("^([-+])?([\\.\\w]+)(?:<(\\w+)>)?$");
  private static final String FP = "([-+]?[0-9]+(?:\\.[0-9]+)?)";
  private static final Pattern DISTANCE_RE =
      Pattern.compile(
          String.format("^([-+])?<distance,([\\.\\w]+),([\\.\\w]+),%s,%s,(mi|km)>$", FP, FP));
  private static final Pattern RANGE_RE = Pattern.compile("([{\\[])(\\S+) TO (\\S+)([}\\]])");

  private static final Logger LOGGER = LogManager.getLogger();

  private static final Set<String> ID_SET = Collections.singleton("_id");

  private static Set<String> defaultFieldsToLoad(final Set<String> fieldsToLoad) {
    if (fieldsToLoad == null) {
      return ID_SET;
    }
    fieldsToLoad.add("_id");
    return fieldsToLoad;
  }

  protected static final Histogram LATENCIES = latencies();

  protected Logger logger;
  private final ThreadLocal<QueryParser> queryParser;

  protected BaseSearchHandler(final Analyzer analyzer) {
    this.queryParser =
        new ThreadLocal<QueryParser>() {

          @Override
          protected QueryParser initialValue() {
            return new Search3QueryParser(analyzer);
          }
        };
  }

  private final Query parse(final SearchRequest request) throws ParseException {
    final Query baseQuery = parse(request.getQuery(), request.getPartition());
    if (request.getDrilldownCount() == 0) {
      return baseQuery;
    }
    final DrillDownQuery drilldownQuery =
        new DrillDownQuery(DocumentBuilder.FACETS_CONFIG, baseQuery);
    for (final Path path : request.getDrilldownList()) {
      final List<String> parts = path.getPartsList();
      drilldownQuery.add(parts.get(0), parts.subList(1, parts.size()).toArray(EMPTY_STRING_ARR));
    }
    return drilldownQuery;
  }

  protected final Query parse(final String queryString, final String partition)
      throws ParseException {
    final Query baseQuery = queryParser.get().parse(queryString);
    if (partition.isEmpty()) {
      return baseQuery;
    } else {
      final BooleanQuery.Builder builder = new BooleanQuery.Builder();
      builder.add(new TermQuery(new Term("_partition", partition)), Occur.MUST);
      builder.add(baseQuery, Occur.MUST);
      return builder.build();
    }
  }

  @Override
  public final SearchResponse search(final SearchRequest request)
      throws IOException, ParseException {
    verifySession(request.getIndex());
    final int limit = request.getLimit();
    final Set<String> fieldsToLoad = toFieldSet(request);
    final boolean staleOk = request.getStale();
    final Sort sort = toSort(request);
    final ScoreDoc after = toAfter(request, sort);
    final Query query = parse(request);

    return withSearcher(
        staleOk,
        searcher -> {
          final MultiCollectorManager manager;
          final CollectorManager<? extends Collector, ? extends TopDocs> resultsManager =
              resultsCollectorManager(searcher, sort, limit, after);
          final FacetsCollectorManager facetsManager;
          final FacetsCollector facetsCollector;

          if (request.getCountsCount() > 0 || request.getRangesCount() > 0) {
            facetsManager = new FacetsCollectorManager();
            manager = new MultiCollectorManager(resultsManager, facetsManager);
          } else {
            manager = new MultiCollectorManager(resultsManager);
          }

          final Highlighter highlighter;
          if (request.getHighlightFieldsCount() > 0) {
            highlighter = getHighlighter(request, query);
          } else {
            highlighter = null;
          }

          final TopDocs topDocs;
          try {
            final Object[] reduces;
            final Histogram.Timer requestTimer = LATENCIES.labels("searches").startTimer();
            try {
              reduces = searcher.search(query, manager);
            } finally {
              requestTimer.observeDuration();
            }
            topDocs = (TopDocs) reduces[0];
            facetsCollector = reduces.length > 1 ? (FacetsCollector) reduces[1] : null;
          } catch (final IllegalStateException e) {
            final String message = e.getMessage();
            if (message != null && message.contains("(expected=NUMERIC)")) {
              throw new IllegalStateException("cannot sort string field as numeric field", e);
            }
            if (message != null && message.contains("(expected=SORTED)")) {
              throw new IllegalStateException("cannot sort numeric field as string field", e);
            }
            throw e;
          }

          final SearchResponse.Builder responseBuilder = SearchResponse.newBuilder();
          responseBuilder.setSession(getSession());
          responseBuilder.setMatches(topDocs.totalHits.value);
          addBookmark(responseBuilder, topDocs);
          for (int i = 0; i < topDocs.scoreDocs.length; i++) {
            final ScoreDoc scoreDoc = topDocs.scoreDocs[i];
            final Document doc;
            if (fieldsToLoad.isEmpty()) {
              doc = searcher.doc(scoreDoc.doc);
            } else {
              doc = searcher.doc(scoreDoc.doc, defaultFieldsToLoad(fieldsToLoad));
            }
            final Hit.Builder hitBuilder = Hit.newBuilder();
            hitBuilder.setId(doc.get("_id"));
            addOrderToHit(hitBuilder, scoreDoc);
            addFieldsToHit(hitBuilder, doc);
            addHighlightsToHit(hitBuilder, doc, request, highlighter);
            responseBuilder.addHits(hitBuilder);
          }

          if (request.getCountsCount() > 0) {
            final SortedSetDocValuesReaderState state =
                new DefaultSortedSetDocValuesReaderState(searcher.getIndexReader());
            final Facets facets = new SortedSetDocValuesFacetCounts(state, facetsCollector);
            for (final String count : request.getCountsList()) {
              final Counts.Builder countsBuilder = Counts.newBuilder();
              final FacetResult facetResult = facets.getTopChildren(1000, count);
              for (final LabelAndValue lv : facetResult.labelValues) {
                countsBuilder.putCounts(lv.label, lv.value.intValue());
              }
              responseBuilder.putCounts(count, countsBuilder.build());
            }
          }

          if (request.getRangesCount() > 0) {
            for (final Entry<String, Ranges> entry : request.getRangesMap().entrySet()) {
              final Map<String, String> map = entry.getValue().getRangesMap();
              final DoubleRange[] ranges = new DoubleRange[map.size()];
              int i = 0;
              for (final Entry<String, String> range : map.entrySet()) {
                final Matcher m = RANGE_RE.matcher(range.getValue());
                if (!m.matches()) {
                  try {
                    throw new ParseException(
                        range.getValue() + " was not a well-formed range specification");
                  } catch (final ParseException e) {
                    throw new IllegalArgumentException(e);
                  }
                }
                final boolean minInclusive = m.group(1).equals("[");
                final double minIn = Double.parseDouble(m.group(2));
                final double maxIn = Double.parseDouble(m.group(3));
                final boolean maxInclusive = m.group(4).equals("]");
                ranges[i++] =
                    new DoubleRange(range.getKey(), minIn, minInclusive, maxIn, maxInclusive);
              }
              final Facets facets =
                  new DoubleRangeFacetCounts(entry.getKey(), facetsCollector, ranges);
              final FacetResult facetResult = facets.getTopChildren(map.size(), entry.getKey());
              final Counts.Builder countsBuilder = Counts.newBuilder();
              for (final LabelAndValue lv : facetResult.labelValues) {
                countsBuilder.putCounts(lv.label, lv.value.intValue());
              }
              responseBuilder.putRanges(entry.getKey(), countsBuilder.build());
            }
          }

          return responseBuilder.build();
        });
  }

  private void addBookmark(final SearchResponse.Builder responseBuilder, final TopDocs topDocs) {
    if (topDocs.scoreDocs.length > 0) {
      final ScoreDoc lastDoc = topDocs.scoreDocs[topDocs.scoreDocs.length - 1];
      final Bookmark bookmark = Bookmark.newBuilder().addAllOrder(toFieldValues(lastDoc)).build();
      responseBuilder.setBookmark(bookmark);
    }
  }

  protected final void addFieldsToHit(final Hit.Builder hitBuilder, final Document doc) {
    for (final IndexableField field : doc) {
      if ("_id".equals(field.name())) {
        continue;
      }
      final HitField.Builder hitFieldBuilder = HitField.newBuilder();
      hitFieldBuilder.setName(field.name());
      final FieldValue.Builder fieldValueBuilder;
      if (field.stringValue() != null) {
        fieldValueBuilder = FieldValue.newBuilder().setString(field.stringValue());
      } else if (field.numericValue() != null) {
        fieldValueBuilder = FieldValue.newBuilder().setDouble(field.numericValue().doubleValue());
      } else {
        continue;
      }
      hitFieldBuilder.setValue(fieldValueBuilder);
      hitBuilder.addFields(hitFieldBuilder);
    }
  }

  protected final void addOrderToHit(final Hit.Builder hitBuilder, final ScoreDoc scoreDoc) {
    hitBuilder.addAllOrder(toFieldValues(scoreDoc));
  }

  protected final void addHighlightsToHit(
      final Hit.Builder hitBuilder,
      final Document doc,
      final SearchRequest request,
      final Highlighter highlighter)
      throws IOException {
    if (request.getHighlightFieldsCount() == 0) {
      return;
    }

    final int highlightNumber =
        request.getHighlightNumber() != 0 ? request.getHighlightNumber() : 1;
    for (final String highlightField : request.getHighlightFieldsList()) {
      final Analyzer analyzer = analyzer(highlightField);
      try {
        final String[] fragments =
            highlighter.getBestFragments(
                analyzer, highlightField, doc.get(highlightField), highlightNumber);
        final Highlights.Builder highlightsBuilder = Highlights.newBuilder();
        highlightsBuilder.setFieldname(highlightField);
        for (final String fragment : fragments) {
          highlightsBuilder.addHighlights(fragment);
        }
        hitBuilder.addHighlights(highlightsBuilder);
      } catch (final InvalidTokenOffsetsException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  private Iterable<FieldValue> toFieldValues(final ScoreDoc scoreDoc) {
    final List<FieldValue> result = new ArrayList<FieldValue>();
    if (scoreDoc instanceof FieldDoc) {
      final Object[] fields = ((FieldDoc) scoreDoc).fields;
      for (int i = 0; i < fields.length; i++) {
        Object field = fields[i];
        if (field instanceof Number) {
          final double value = ((Number) field).doubleValue();
          result.add(FieldValue.newBuilder().setDouble(value).build());
        } else if (field instanceof BytesRef) {
          final String value = ((BytesRef) field).utf8ToString();
          result.add(FieldValue.newBuilder().setString(value).build());
        } else if (field == null) {
          result.add(NULL_VALUE);
        } else {
          throw new IllegalArgumentException("Unknown order value: " + field);
        }
      }
    } else {
      result.add(FieldValue.newBuilder().setFloat(scoreDoc.score).build());
    }
    result.add(FieldValue.newBuilder().setInt(scoreDoc.doc).build());
    return result;
  }

  protected abstract <T> T withSearcher(final boolean staleOk, final IOFunction<IndexSearcher, T> f)
      throws IOException;

  protected abstract String getSession();

  protected final SessionResponse sessionResponse() {
    return SessionResponse.newBuilder().setSession(getSession()).build();
  }

  protected static Set<String> toFieldSet(final SearchRequest request) {
    return new HashSet<String>(request.getIncludeFieldsList());
  }

  protected static Sort toSort(final SearchRequest request) throws ParseException {
    return toSort(request.getSort());
  }

  protected static Sort toSort(final GroupSearchRequest request) throws ParseException {
    return toSort(request.getGroupSort());
  }

  protected static Sort toSort(final Search3.Sort sort) throws ParseException {
    if (sort.getFieldsCount() == 0) {
      return null;
    }

    final SortField[] sortFields = new SortField[sort.getFieldsCount()];
    for (int i = 0; i < sort.getFieldsCount(); i++) {
      switch (sort.getFields(i)) {
        case "<score>":
          sortFields[i] = INVERSE_FIELD_SCORE;
          continue;
        case "-<score>":
          sortFields[i] = SortField.FIELD_SCORE;
          continue;
        case "<doc>":
          sortFields[i] = SortField.FIELD_DOC;
          continue;
        case "-<doc>":
          sortFields[i] = INVERSE_FIELD_DOC;
          continue;
      }

      Matcher m = DISTANCE_RE.matcher(sort.getFields(i));
      if (m.matches()) {
        final boolean reverse = "-".equals(m.group(1));
        final String lonField = m.group(2);
        final String latField = m.group(3);
        final double lon = Double.parseDouble(m.group(4));
        final double lat = Double.parseDouble(m.group(5));
        final String units = m.group(6);
        final double radius;
        switch (units) {
          case "mi":
            radius = DistanceUtils.EARTH_EQUATORIAL_RADIUS_MI;
            break;
          case "km":
          default:
            radius = DistanceUtils.EARTH_EQUATORIAL_RADIUS_KM;
            break;
        }
        final double degToKm = DistanceUtils.degrees2Dist(1, radius);

        final SpatialContext ctx = SpatialContext.GEO;
        final Point from = new PointImpl(lon, lat, ctx);
        final DistanceValueSource source =
            new DistanceValueSource(ctx, lonField, latField, degToKm, from);
        sortFields[i] = source.getSortField(reverse);
      }

      m = SORT_FIELD_RE.matcher(sort.getFields(i));
      if (m.matches()) {
        final String fieldTypeStr = m.group(3) == null ? "number" : m.group(3);
        final SortField.Type fieldType;
        switch (fieldTypeStr) {
          case "string":
            fieldType = SortField.Type.STRING;
            break;
          case "number":
            fieldType = SortField.Type.DOUBLE;
            break;
          default:
            throw new ParseException("Unrecognized type: " + m.group(3));
        }
        final boolean reverse = "-".equals(m.group(1));

        sortFields[i] = new SortField(m.group(2), fieldType, reverse);
      }
    }
    return new Sort(sortFields);
  }

  protected static ScoreDoc toAfter(final SearchRequest request, final Sort sort) {
    if (!request.hasBookmark()) {
      return null;
    }

    final Bookmark bookmark = request.getBookmark();

    // Default sort order (by relevance).
    if (sort == null) {
      final float score = bookmark.getOrder(0).getFloat();
      final int doc = bookmark.getOrder(1).getInt();
      return new ScoreDoc(doc, score);
    }

    // Custom sort order.
    final Object[] fields = new Object[bookmark.getOrderCount() - 1];
    for (int i = 0; i < fields.length; i++) {
      final FieldValue value = bookmark.getOrder(i);
      switch (value.getValueCase()) {
        case BOOL:
          fields[i] = value.getBool();
          break;
        case DOUBLE:
          fields[i] = value.getDouble();
          break;
        case FLOAT:
          fields[i] = value.getFloat();
          break;
        case INT:
          fields[i] = value.getInt();
          break;
        case LONG:
          fields[i] = value.getLong();
          break;
        case STRING:
          fields[i] = new BytesRef(value.getString());
          break;
        default:
          throw new IllegalArgumentException(value + " is malformed in bookmark");
      }
    }
    final int doc = bookmark.getOrder(bookmark.getOrderCount() - 1).getInt();
    return new FieldDoc(doc, Float.NaN, fields);
  }

  protected static Document toDoc(final DocumentUpdateRequest request) throws IOException {
    final DocumentBuilder builder = new DocumentBuilder();
    builder.addString("_id", request.getId(), true, false);

    if (!request.getPartition().isEmpty()) {
      builder.addString("_partition", request.getPartition(), false, false);
    }

    for (final DocumentField field : request.getFieldsList()) {
      final String name = field.getName();
      final FieldValue value = field.getValue();
      final boolean analyzed = field.getAnalyzed();
      final boolean store = field.getStore();
      final boolean facet = field.getFacet();

      switch (value.getValueCase()) {
        case BOOL:
          builder.addBoolean(name, value.getBool(), store);
          break;
        case INT:
          builder.addLong(name, value.getInt(), store);
          break;
        case LONG:
          builder.addLong(name, value.getLong(), store);
          break;
        case FLOAT:
          builder.addDouble(name, value.getFloat(), store);
          break;
        case DOUBLE:
          builder.addDouble(name, value.getDouble(), store);
          break;
        case STRING:
          if (analyzed) {
            builder.addText(name, value.getString(), store, facet);
          } else {
            builder.addString(name, value.getString(), store, facet);
          }
          break;
        default:
          throw new IllegalArgumentException(name + " has no value.");
      }
    }
    return builder.build();
  }

  protected static UpdateSeq seq(final String value) {
    return UpdateSeq.newBuilder().setSeq(value).build();
  }

  protected final void verifySession(final Index index) {
    final String session = index.getSession();
    if (session.isEmpty()) {
      return;
    }
    if (getSession().equals(session)) {
      return;
    }
    throw new SessionMismatchException("session mismatch");
  }

  private CollectorManager<? extends Collector, ? extends TopDocs> resultsCollectorManager(
      final IndexSearcher searcher, final Sort sort, final int limit, final ScoreDoc after)
      throws IOException {
    if (limit == 0) {
      return totalHitCountCollectorManager();
    } else if (sort == null) {
      return topScoreDocCollectorManager(searcher, after, limit);
    } else {
      return topFieldDocCollectorManager(searcher, (FieldDoc) after, sort, limit);
    }
  }

  private CollectorManager<TotalHitCountCollector, TopDocs> totalHitCountCollectorManager() {
    return new CollectorManager<TotalHitCountCollector, TopDocs>() {

      @Override
      public TotalHitCountCollector newCollector() throws IOException {
        return new TotalHitCountCollector();
      }

      @Override
      public TopDocs reduce(final Collection<TotalHitCountCollector> collectors)
          throws IOException {
        int count = 0;
        for (TotalHitCountCollector collector : collectors) {
          count += collector.getTotalHits();
        }
        return new TopDocs(new TotalHits(count, Relation.EQUAL_TO), EMPTY_SCORE_DOC);
      }
    };
  }

  private CollectorManager<TopScoreDocCollector, TopDocs> topScoreDocCollectorManager(
      final IndexSearcher searcher, final ScoreDoc after, final int numHits) {
    final int limit = Math.max(1, searcher.getIndexReader().maxDoc());
    if (after != null && after.doc >= limit) {
      throw new IllegalArgumentException(
          "after.doc exceeds the number of documents in the reader: after.doc="
              + after.doc
              + " limit="
              + limit);
    }

    final int cappedNumHits = Math.min(numHits, limit);

    return new CollectorManager<TopScoreDocCollector, TopDocs>() {

      @Override
      public TopScoreDocCollector newCollector() throws IOException {
        return TopScoreDocCollector.create(cappedNumHits, after, Integer.MAX_VALUE);
      }

      @Override
      public TopDocs reduce(final Collection<TopScoreDocCollector> collectors) throws IOException {
        final TopDocs[] topDocs = new TopDocs[collectors.size()];
        int i = 0;
        for (TopScoreDocCollector collector : collectors) {
          topDocs[i++] = collector.topDocs();
        }
        return TopDocs.merge(0, cappedNumHits, topDocs, true);
      }
    };
  }

  private CollectorManager<TopFieldCollector, TopFieldDocs> topFieldDocCollectorManager(
      final IndexSearcher searcher, final FieldDoc after, final Sort sort, final int numHits)
      throws IOException {
    final int limit = Math.max(1, searcher.getIndexReader().maxDoc());
    if (after != null && after.doc >= limit) {
      throw new IllegalArgumentException(
          "after.doc exceeds the number of documents in the reader: after.doc="
              + after.doc
              + " limit="
              + limit);
    }
    final int cappedNumHits = Math.min(numHits, limit);
    final Sort rewrittenSort = sort.rewrite(searcher);
    return new CollectorManager<TopFieldCollector, TopFieldDocs>() {

      @Override
      public TopFieldCollector newCollector() throws IOException {
        return TopFieldCollector.create(rewrittenSort, cappedNumHits, after, Integer.MAX_VALUE);
      }

      @Override
      public TopFieldDocs reduce(final Collection<TopFieldCollector> collectors)
          throws IOException {
        final TopFieldDocs[] topDocs = new TopFieldDocs[collectors.size()];
        int i = 0;
        for (TopFieldCollector collector : collectors) {
          topDocs[i++] = collector.topDocs();
        }
        return TopDocs.merge(rewrittenSort, 0, cappedNumHits, topDocs, true);
      }
    };
  }

  private Highlighter getHighlighter(final SearchRequest request, final Query query) {
    final String preTag =
        !request.getHighlightPreTag().isEmpty() ? request.getHighlightPreTag() : "<em>";
    final String postTag =
        !request.getHighlightPostTag().isEmpty() ? request.getHighlightPostTag() : "</em>";
    final int highlightSize = request.getHighlightSize() != 0 ? request.getHighlightSize() : 100;
    final Formatter formatter = new SimpleHTMLFormatter(preTag, postTag);
    final Highlighter result = new Highlighter(formatter, new QueryScorer(query));

    if (highlightSize > 0) {
      result.setTextFragmenter(new SimpleFragmenter(highlightSize));
    }

    return result;
  }

  private Analyzer analyzer(final String fieldName) {
    final Analyzer analyzer = queryParser.get().getAnalyzer();
    if (analyzer instanceof PerFieldAnalyzer) {
      return ((PerFieldAnalyzer) analyzer).getWrappedAnalyzer(fieldName);
    }
    return analyzer;
  }

  private static Histogram latencies() {
    return Histogram.build()
        .name("search3_latency_seconds")
        .labelNames("type")
        .help("search3 latencies")
        .register();
  }
}
