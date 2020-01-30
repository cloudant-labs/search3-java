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
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.distance.DistanceCalculator;
import org.locationtech.spatial4j.shape.Point;

/*
 * This is lucene spatial's DistanceValueSource but with configurable x and y field names to better
 * suit our existing API.
 */
public class DistanceValueSource extends DoubleValuesSource {

  private final SpatialContext ctx;
  private final String lon;
  private final String lat;
  private final double multiplier;
  private final double nullValue;
  private final Point from;

  public DistanceValueSource(
      final SpatialContext ctx,
      final String lon,
      final String lat,
      final double multiplier,
      final Point from) {
    this.ctx = ctx;
    this.lon = lon;
    this.lat = lat;
    this.multiplier = multiplier;
    this.nullValue = 180 * multiplier;
    this.from = from;
  }

  @Override
  public boolean isCacheable(final LeafReaderContext ctx) {
    return DocValues.isCacheable(ctx, lon, lat);
  }

  @Override
  public DoubleValues getValues(final LeafReaderContext readerContext, final DoubleValues scores)
      throws IOException {
    LeafReader reader = readerContext.reader();

    final NumericDocValues ptX = DocValues.getNumeric(reader, lon);
    final NumericDocValues ptY = DocValues.getNumeric(reader, lat);

    return DoubleValues.withDefault(
        new DoubleValues() {

          private final Point from = DistanceValueSource.this.from;
          private final DistanceCalculator calculator = ctx.getDistCalc();

          @Override
          public double doubleValue() throws IOException {
            double x = Double.longBitsToDouble(ptX.longValue());
            double y = Double.longBitsToDouble(ptY.longValue());
            return calculator.distance(from, x, y) * multiplier;
          }

          @Override
          public boolean advanceExact(int doc) throws IOException {
            return ptX.advanceExact(doc) && ptY.advanceExact(doc);
          }
        },
        nullValue);
  }

  @Override
  public boolean needsScores() {
    return false;
  }

  @Override
  public DoubleValuesSource rewrite(IndexSearcher reader) throws IOException {
    return this;
  }

  @Override
  public int hashCode() {
    return from.hashCode();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    DistanceValueSource that = (DistanceValueSource) o;

    if (!from.equals(that.from)) return false;
    if (multiplier != that.multiplier) return false;

    return true;
  }

  @Override
  public String toString() {
    return "DistanceValueSource(" + from + ")";
  }
}
