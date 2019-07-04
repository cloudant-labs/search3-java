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
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
import org.apache.lucene.util.BytesRef;

public final class DocumentBuilder {

    private Document document;

    public DocumentBuilder() {
    }

    public DocumentBuilder addString(final String name, final String value, final boolean store) {
        doc().add(new StringField(name, value, toStore(store)));
        doc().add(new SortedDocValuesField(name, new BytesRef(value)));
        return this;
    }

    public DocumentBuilder addText(final String name, final String value, final boolean store, final boolean facet) {
        doc().add(new TextField(name, value, toStore(store)));
        if (facet) {
            doc().add(new SortedSetDocValuesFacetField(name, value));
        }
        return this;
    }

    public DocumentBuilder addBoolean(final String name, final boolean value, final boolean store) {
        doc().add(new StringField(name, value ? "true" : "false", toStore(store)));
        return this;
    }

    public DocumentBuilder addDouble(final String name, final double value, final boolean store) {
        // For querying.
        doc().add(new DoublePoint(name, value));

        // For sorting and facets.
        doc().add(new DoubleDocValuesField(name, value));

        // For retrieval.
        if (store) {
            doc().add(new StoredField(name, value));
        }
        return this;
    }

    public Document build() throws IOException {
        final Document result = doc();
        this.document = null;
        new FacetsConfig().build(result);
        return result;
    }

    private Store toStore(final boolean store) {
        return store ? Store.YES : Store.NO;
    }

    private Document doc() {
        if (this.document == null) {
            this.document = new Document();
        }
        return this.document;
    }

}
