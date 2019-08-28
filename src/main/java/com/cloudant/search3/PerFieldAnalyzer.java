/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.cloudant.search3;

import java.util.Collections;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;

/**
 * Identical to PerFieldAnalyzerWrapper except public.
 */
public final class PerFieldAnalyzer extends DelegatingAnalyzerWrapper {

    private final Analyzer defaultAnalyzer;
    private final Map<String, Analyzer> fieldAnalyzers;

    public PerFieldAnalyzer(Analyzer defaultAnalyzer,
            Map<String, Analyzer> fieldAnalyzers) {
          super(PER_FIELD_REUSE_STRATEGY);
          this.defaultAnalyzer = defaultAnalyzer;
          this.fieldAnalyzers = (fieldAnalyzers != null) ? fieldAnalyzers : Collections.<String, Analyzer>emptyMap();
        }

    public Analyzer getDefault() {
        return defaultAnalyzer;
    }

    @Override
    public Analyzer getWrappedAnalyzer(String fieldName) {
        Analyzer analyzer = fieldAnalyzers.get(fieldName);
        return (analyzer != null) ? analyzer : defaultAnalyzer;
    }

    @Override
    public String toString() {
        return "PerFieldAnalyzerWrapper(" + fieldAnalyzers + ", default=" + defaultAnalyzer + ")";
    }

}
