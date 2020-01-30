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

import static org.junit.Assert.assertEquals;

import org.apache.lucene.document.Document;
import org.junit.Test;

public class DocumentBuilderTest {

  @Test
  public void addBoolean() throws Exception {
    final DocumentBuilder builder = new DocumentBuilder();
    builder.addBoolean("foo", true, true);
    final Document doc = builder.build();
    assertEquals("true", doc.get("foo"));
  }

  @Test
  public void addDouble() throws Exception {
    final DocumentBuilder builder = new DocumentBuilder();
    builder.addDouble("foo", 3.41, true);
    final Document doc = builder.build();
    assertEquals(3, doc.getFields().size());
  }

  @Test
  public void addText() throws Exception {
    final DocumentBuilder builder = new DocumentBuilder();
    builder.addText("foo", "bar", false, false);
    final Document doc = builder.build();
    assertEquals(2, doc.getFields().size());
  }
}
