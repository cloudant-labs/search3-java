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

import com.apple.foundationdb.Database;
import com.apple.foundationdb.subspace.Subspace;
import java.io.IOException;
import org.apache.lucene.analysis.Analyzer;

public interface SearchHandlerFactory {

  SearchHandler open(final Database db, final Subspace index, final Analyzer analyzer)
      throws IOException;
}
