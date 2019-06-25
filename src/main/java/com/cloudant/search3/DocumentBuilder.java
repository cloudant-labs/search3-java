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

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
import org.apache.lucene.index.IndexableField;

public final class DocumentBuilder {

    private Document document;

    public DocumentBuilder() {
        this.document = new Document();
    }

    public DocumentBuilder addString(final String name, final String value) {
        add(new StringField(name, value, Store.YES));
        return this;
    }

    public DocumentBuilder addText(final String name, final String value, final boolean store, final boolean facet) {
        add(new TextField(name, value, store ? Store.YES : Store.NO));
        if (facet) {
            add(new SortedSetDocValuesFacetField(name, value));
        }
        return this;
    }

    public DocumentBuilder addBoolean(final String name, final boolean value, final boolean store) {
        add(new StringField(name, value ? "true" : "false", store ? Store.YES : Store.NO));
        return this;
    }

    public DocumentBuilder addDouble(final String name, final double value, final boolean store) {
        // For querying.
        add(new DoublePoint(name, value));
        // For sorting and facets.
        add(new DoubleDocValuesField(name, value));
        if (store) {
            add(new StoredField(name, value));
        }
        return this;
    }

    public Document build() throws IOException {
        new FacetsConfig().build(this.document);
        final Document result = this.document;
        this.document = new Document();
        return result;
    }

    private void add(final IndexableField field) {
        this.document.add(field);
    }

}
