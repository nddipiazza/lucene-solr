/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.search;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.LongBitSet;
import org.apache.lucene.util.LongValues;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.join.MultiValueTermOrdinalCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostFilterLocalJoinQuery extends JoinQuery implements PostFilter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private boolean cache;
  private int cost;
  private boolean cacheSep;

  private String joinField;

  public PostFilterLocalJoinQuery(String joinField, String coreName, Query subQuery) {
    super(joinField, joinField, coreName, subQuery);
    this.joinField = joinField;
  }

  @Override
  public DelegatingCollector getFilterCollector(IndexSearcher searcher) {
    log.debug("Running local join query using postfilter");
    final SolrIndexSearcher solrSearcher = (SolrIndexSearcher) searcher;
    try {

      final SortedSetDocValues fromValues = validateAndFetchDocValues(solrSearcher, joinField, "from");
      final SortedSetDocValues toValues = validateAndFetchDocValues(solrSearcher, joinField, "to");

      final LongBitSet ordBitSet = new LongBitSet(fromValues.getValueCount());

      final Collector collector = new MultiValueTermOrdinalCollector(joinField, fromValues, ordBitSet);
      solrSearcher.search(q, collector);

      return new LocalJoinQueryCollector(toValues, ordBitSet);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean getCache() {
    return false;
  }

  public int hashCode() {
    return 1;
  }

  @Override
  public void setCache(boolean cache) {
    this.cache = cache;
  }

  @Override
  public int getCost() {
    return 102;
  }

  @Override
  public void setCost(int cost) {
    this.cost = cost;
  }

  @Override
  public boolean getCacheSep() {
    return cacheSep;
  }

  @Override
  public void setCacheSep(boolean cacheSep) {
    this.cacheSep = cacheSep;
  }

  private SortedSetDocValues validateAndFetchDocValues(SolrIndexSearcher solrSearcher, String fieldName, String querySide) throws IOException {
    final IndexSchema schema = solrSearcher.getSchema();
    final SchemaField field = schema.getFieldOrNull(fieldName);
    if (field == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, querySide + " field '" + fieldName + "' does not exist");
    }

    if (!field.hasDocValues()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "'top-level' join queries require both 'from' and 'to' fields to have docValues, but " + querySide +
              " field [" + fieldName +  "] does not.");
    }

    final LeafReader leafReader = solrSearcher.getSlowAtomicReader();
    if (field.multiValued()) {
      return DocValues.getSortedSet(leafReader, fieldName);
    }
    return DocValues.singleton(DocValues.getSorted(leafReader, fieldName));
  }

  public static QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new QParser(qstr, localParams, params, req) {
      @Override
      public Query parse() throws SyntaxError {
        final String fromField = localParams.get("from");
        final String v = localParams.get(CommonParams.VALUE);

        LocalSolrQueryRequest otherReq = new LocalSolrQueryRequest(req.getCore(), params);
        QParser fromQueryParser = QParser.getParser(v, otherReq);
        Query fromQuery = fromQueryParser.getQuery();
        return new PostFilterLocalJoinQuery(fromField, null, fromQuery);

      }
    };
  }

  private static class LocalJoinQueryCollector extends DelegatingCollector {
    private LeafCollector leafCollector;
    private LongBitSet topLevelDocValuesBitSet;
    private OrdinalMap ordinalMap;
    private SortedSetDocValues[] sortedDocValues;
    private SortedSetDocValues leafDocValues;
    private LongValues longValues;

    public LocalJoinQueryCollector(SortedSetDocValues topLevelDocValues, LongBitSet topLevelDocValuesBitSet) {
      this.topLevelDocValuesBitSet = topLevelDocValuesBitSet;
      this.ordinalMap = ((MultiDocValues.MultiSortedSetDocValues)topLevelDocValues).mapping;
      this.sortedDocValues = ((MultiDocValues.MultiSortedSetDocValues)topLevelDocValues).values;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
      leafCollector.setScorer(scorer);
    }

    @Override
    public void doSetNextReader(LeafReaderContext context) throws IOException {
      this.leafCollector = delegate.getLeafCollector(context);
      this.docBase = context.docBase;
      this.leafDocValues = sortedDocValues[context.ord];
      this.longValues = ordinalMap.getGlobalOrds(context.ord);
    }

    @Override
    public void collect(int doc) throws IOException {
      if (this.leafDocValues.advanceExact(doc)) {
        long ord;
        while ((ord = leafDocValues.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
          long globalOrd = longValues.get(ord);
          if (topLevelDocValuesBitSet.get(globalOrd)) {
            leafCollector.collect(doc);
            break;
          }
        }
      }
    }
  }
}