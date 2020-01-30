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

import java.util.regex.Pattern;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;

/**
 * 1. Perform a numeric range query if start and end look like numbers. 2. Did _not_ import the
 * setLowercaseExpandedTerms(false) for KeywordAnalyzer from ClouseauQueryParser as this is now
 * fixed in Lucene, the normalize() method of the analyzer is where lower-casing (or not) is applied
 * and the KeyWordAnalyzer does not perform case-folding for expanded terms.
 */
public final class Search3QueryParser extends QueryParser {

  private static final Pattern FP_REGEX;

  static {
    final String digits = "(\\p{Digit}+)";
    final String hexDigits = "(\\p{XDigit}+)";
    final String exp = "[eE][+-]?" + digits;
    final String regexp =
        ("[\\x00-\\x20]*"
            + "[+-]?("
            + "NaN|"
            + "Infinity|"
            + "((("
            + digits
            + "(\\.)?("
            + digits
            + "?)("
            + exp
            + ")?)|"
            + "(\\.("
            + digits
            + ")("
            + exp
            + ")?)|"
            + "(("
            + "(0[xX]"
            + hexDigits
            + "(\\.)?)|"
            + "(0[xX]"
            + hexDigits
            + "?(\\.)"
            + hexDigits
            + ")"
            + ")[pP][+-]?"
            + digits
            + "))"
            + "[fFdD]?))"
            + "[\\x00-\\x20]*");
    FP_REGEX = Pattern.compile(regexp);
  }

  public Search3QueryParser(final Analyzer analyzer) {
    super("default", analyzer);
  }

  @Override
  protected Query getRangeQuery(
      String field, String part1, String part2, boolean startInclusive, boolean endInclusive)
      throws ParseException {
    if (isNumber(part1) && isNumber(part2)) {
      double start = Double.parseDouble(part1);
      start = startInclusive ? start : DoublePoint.nextUp(start);

      double end = Double.parseDouble(part2);
      end = endInclusive ? end : DoublePoint.nextDown(end);

      return DoublePoint.newRangeQuery(field, start, end);
    } else {
      return super.getRangeQuery(field, part1, part2, startInclusive, endInclusive);
    }
  }

  private boolean isNumber(final String str) {
    return FP_REGEX.matcher(str).matches();
  }
}
