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

import java.lang.reflect.Field;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.fi.FinnishAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Test;

import com.cloudant.search3.grpc.Search3.AnalyzerSpec;
import com.cloudant.search3.grpc.Search3.Index;

public class SupportedAnalyzersTest {


    @Test
    public void testEnglish() throws Exception {
        assertDefault(single("english"), EnglishAnalyzer.class);
    }

    @Test
    public void testStandard() throws Exception {
        assertDefault(single("standard"), StandardAnalyzer.class);
    }

    @Test
    public void testFinnish() throws Exception {
        assertDefault(single("finnish"), FinnishAnalyzer.class);
    }

    private Analyzer single(final String name) {
        final Index.Builder builder = Index.newBuilder();
        builder.setDefault(AnalyzerSpec.newBuilder().setName(name));
        return SupportedAnalyzers.createAnalyzer(builder.build());
    }

    private void assertDefault(final Analyzer analyzer, final Class<?> clazz) throws Exception {
        assertThat(analyzer, instanceOf(PerFieldAnalyzerWrapper.class));
        final Field f = analyzer.getClass().getDeclaredField("defaultAnalyzer");
        f.setAccessible(true);
        assertThat(f.get(analyzer), instanceOf(clazz));
    }

}
