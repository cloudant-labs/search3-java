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

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.fi.FinnishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Test;

import com.cloudant.search3.grpc.Search3.AnalyzerSpec;
import com.cloudant.search3.grpc.Search3.OpenIndex;

public class SupportedAnalyzersTest {

    @Test
    public void testEnglish() {
        assertThat(single("english"), instanceOf(EnglishAnalyzer.class));
    }

    @Test
    public void testStandard() {
        assertThat(single("standard"), instanceOf(StandardAnalyzer.class));
    }

    @Test
    public void testFinnish() {
        assertThat(single("finnish"), instanceOf(FinnishAnalyzer.class));
    }

    private Analyzer single(final String name) {
        final OpenIndex.Builder builder = OpenIndex.newBuilder();
        builder.setDefault(AnalyzerSpec.newBuilder().setName(name));
        final OpenIndex openIndex = builder.build();
        return SupportedAnalyzers.createAnalyzer(openIndex);
    }

}
