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

import com.cloudant.search3.grpc.Search3.AnalyzerSpec;
import com.cloudant.search3.grpc.Search3.Index;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.ar.ArabicAnalyzer;
import org.apache.lucene.analysis.bg.BulgarianAnalyzer;
import org.apache.lucene.analysis.br.BrazilianAnalyzer;
import org.apache.lucene.analysis.ca.CatalanAnalyzer;
import org.apache.lucene.analysis.cjk.CJKAnalyzer;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.cz.CzechAnalyzer;
import org.apache.lucene.analysis.da.DanishAnalyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.el.GreekAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.analysis.eu.BasqueAnalyzer;
import org.apache.lucene.analysis.fa.PersianAnalyzer;
import org.apache.lucene.analysis.fi.FinnishAnalyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.ga.IrishAnalyzer;
import org.apache.lucene.analysis.gl.GalicianAnalyzer;
import org.apache.lucene.analysis.hi.HindiAnalyzer;
import org.apache.lucene.analysis.hu.HungarianAnalyzer;
import org.apache.lucene.analysis.hy.ArmenianAnalyzer;
import org.apache.lucene.analysis.id.IndonesianAnalyzer;
import org.apache.lucene.analysis.it.ItalianAnalyzer;
import org.apache.lucene.analysis.ja.JapaneseAnalyzer;
import org.apache.lucene.analysis.ja.JapaneseTokenizer;
import org.apache.lucene.analysis.lv.LatvianAnalyzer;
import org.apache.lucene.analysis.nl.DutchAnalyzer;
import org.apache.lucene.analysis.no.NorwegianAnalyzer;
import org.apache.lucene.analysis.pl.PolishAnalyzer;
import org.apache.lucene.analysis.pt.PortugueseAnalyzer;
import org.apache.lucene.analysis.ro.RomanianAnalyzer;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.standard.ClassicAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.UAX29URLEmailAnalyzer;
import org.apache.lucene.analysis.sv.SwedishAnalyzer;
import org.apache.lucene.analysis.th.ThaiAnalyzer;
import org.apache.lucene.analysis.tr.TurkishAnalyzer;

public final class SupportedAnalyzers {

  private SupportedAnalyzers() {}

  public static Analyzer createAnalyzer(final Index index) {
    final Analyzer defaultAnalyzer;
    if (index.hasDefault()) {
      defaultAnalyzer = single(index.getDefault());
    } else {
      defaultAnalyzer = new StandardAnalyzer();
    }

    final Map<String, Analyzer> fieldAnalyzers = new HashMap<String, Analyzer>();
    fieldAnalyzers.put("_id", new KeywordAnalyzer());
    fieldAnalyzers.put("_partition", new KeywordAnalyzer());

    if (index.getPerFieldCount() >= 0) {
      index
          .getPerFieldMap()
          .entrySet()
          .forEach(
              e -> {
                fieldAnalyzers.put(e.getKey(), single(e.getValue()));
              });
    }
    return new PerFieldAnalyzer(defaultAnalyzer, fieldAnalyzers);
  }

  public static Analyzer single(final AnalyzerSpec analyzerSpec) {
    final CharArraySet stopwords;
    if (analyzerSpec.getStopwordsCount() == 0) {
      stopwords = null;
    } else {
      stopwords = new CharArraySet(analyzerSpec.getStopwordsList(), false);
    }

    switch (analyzerSpec.getName()) {
      case "keyword":
        return new KeywordAnalyzer();
      case "simple":
        return new SimpleAnalyzer();
      case "whitespace":
        return new WhitespaceAnalyzer();
      case "arabic":
        if (stopwords == null) {
          return new ArabicAnalyzer();
        } else {
          return new ArabicAnalyzer(stopwords);
        }
      case "bulgarian":
        if (stopwords == null) {
          return new BulgarianAnalyzer();
        } else {
          return new BulgarianAnalyzer(stopwords);
        }
      case "brazilian":
        if (stopwords == null) {
          return new BrazilianAnalyzer();
        } else {
          return new BrazilianAnalyzer(stopwords);
        }
      case "catalan":
        if (stopwords == null) {
          return new CatalanAnalyzer();
        } else {
          return new CatalanAnalyzer(stopwords);
        }
      case "cjk":
        if (stopwords == null) {
          return new CJKAnalyzer();
        } else {
          return new CJKAnalyzer(stopwords);
        }
      case "chinese":
        if (stopwords == null) {
          return new SmartChineseAnalyzer();
        } else {
          return new SmartChineseAnalyzer(stopwords);
        }
      case "czech":
        if (stopwords == null) {
          return new CzechAnalyzer();
        } else {
          return new CzechAnalyzer(stopwords);
        }
      case "danish":
        if (stopwords == null) {
          return new DanishAnalyzer();
        } else {
          return new DanishAnalyzer(stopwords);
        }
      case "german":
        if (stopwords == null) {
          return new GermanAnalyzer();
        } else {
          return new GermanAnalyzer(stopwords);
        }
      case "greek":
        if (stopwords == null) {
          return new GreekAnalyzer();
        } else {
          return new GreekAnalyzer(stopwords);
        }
      case "english":
        if (stopwords == null) {
          return new EnglishAnalyzer();
        } else {
          return new EnglishAnalyzer(stopwords);
        }
      case "spanish":
        if (stopwords == null) {
          return new SpanishAnalyzer();
        } else {
          return new SpanishAnalyzer(stopwords);
        }
      case "basque":
        if (stopwords == null) {
          return new BasqueAnalyzer();
        } else {
          return new BasqueAnalyzer(stopwords);
        }
      case "persian":
        if (stopwords == null) {
          return new PersianAnalyzer();
        } else {
          return new PersianAnalyzer(stopwords);
        }
      case "finnish":
        if (stopwords == null) {
          return new FinnishAnalyzer();
        } else {
          return new FinnishAnalyzer(stopwords);
        }
      case "french":
        if (stopwords == null) {
          return new FrenchAnalyzer();
        } else {
          return new FrenchAnalyzer(stopwords);
        }
      case "irish":
        if (stopwords == null) {
          return new IrishAnalyzer();
        } else {
          return new IrishAnalyzer(stopwords);
        }
      case "galician":
        if (stopwords == null) {
          return new GalicianAnalyzer();
        } else {
          return new GalicianAnalyzer(stopwords);
        }
      case "hindi":
        if (stopwords == null) {
          return new HindiAnalyzer();
        } else {
          return new HindiAnalyzer(stopwords);
        }
      case "hungarian":
        if (stopwords == null) {
          return new HungarianAnalyzer();
        } else {
          return new HungarianAnalyzer(stopwords);
        }
      case "armenian":
        if (stopwords == null) {
          return new ArmenianAnalyzer();
        } else {
          return new ArmenianAnalyzer(stopwords);
        }
      case "indonesian":
        if (stopwords == null) {
          return new IndonesianAnalyzer();
        } else {
          return new IndonesianAnalyzer(stopwords);
        }
      case "italian":
        if (stopwords == null) {
          return new ItalianAnalyzer();
        } else {
          return new ItalianAnalyzer(stopwords);
        }
      case "japanese":
        if (stopwords == null) {
          return new JapaneseAnalyzer();
        } else {
          return new JapaneseAnalyzer(
              null,
              JapaneseTokenizer.DEFAULT_MODE,
              stopwords,
              JapaneseAnalyzer.getDefaultStopTags());
        }
      case "latvian":
        if (stopwords == null) {
          return new LatvianAnalyzer();
        } else {
          return new LatvianAnalyzer(stopwords);
        }
      case "dutch":
        if (stopwords == null) {
          return new DutchAnalyzer();
        } else {
          return new DutchAnalyzer(stopwords);
        }
      case "norwegian":
        if (stopwords == null) {
          return new NorwegianAnalyzer();
        } else {
          return new NorwegianAnalyzer(stopwords);
        }
      case "polish":
        if (stopwords == null) {
          return new PolishAnalyzer();
        } else {
          return new PolishAnalyzer(stopwords);
        }
      case "portuguese":
        if (stopwords == null) {
          return new PortugueseAnalyzer();
        } else {
          return new PortugueseAnalyzer(stopwords);
        }
      case "romanian":
        if (stopwords == null) {
          return new RomanianAnalyzer();
        } else {
          return new RomanianAnalyzer(stopwords);
        }
      case "russian":
        if (stopwords == null) {
          return new RussianAnalyzer();
        } else {
          return new RussianAnalyzer(stopwords);
        }
      case "classic":
        if (stopwords == null) {
          return new ClassicAnalyzer();
        } else {
          return new ClassicAnalyzer(stopwords);
        }
      case "standard":
        if (stopwords == null) {
          return new StandardAnalyzer();
        } else {
          return new StandardAnalyzer(stopwords);
        }
      case "email":
        if (stopwords == null) {
          return new UAX29URLEmailAnalyzer();
        } else {
          return new UAX29URLEmailAnalyzer(stopwords);
        }
      case "swedish":
        if (stopwords == null) {
          return new SwedishAnalyzer();
        } else {
          return new SwedishAnalyzer(stopwords);
        }
      case "thai":
        if (stopwords == null) {
          return new ThaiAnalyzer();
        } else {
          return new ThaiAnalyzer(stopwords);
        }
      case "turkish":
        if (stopwords == null) {
          return new TurkishAnalyzer();
        } else {
          return new TurkishAnalyzer(stopwords);
        }
      default:
        throw new IllegalArgumentException("No analyzer known as " + analyzerSpec.getName());
    }
  }
}
