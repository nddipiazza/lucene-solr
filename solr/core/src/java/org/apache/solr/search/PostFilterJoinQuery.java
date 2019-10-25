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

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongBitSet;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostFilterJoinQuery extends JoinQuery implements PostFilter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private boolean cache;
  private int cost;
  private boolean cacheSep;
  RefCounted<SolrIndexSearcher> fromRef;
  private ResponseBuilder rb;
  private SolrIndexSearcher fromSearcher;
  private SolrIndexSearcher toSearcher;

  public PostFilterJoinQuery(String fromField, String toField, String coreName, Query subQuery) {
    super(fromField, toField, coreName, subQuery);
  }

  @Override
  public DelegatingCollector getFilterCollector(IndexSearcher searcher) {
    log.info("Running join query using postfilter");
    final SolrIndexSearcher solrSearcher = (SolrIndexSearcher) searcher;
    try {
      initializeSearchers(solrSearcher);
      ensureJoinFieldExistsAndHasDocValues(fromSearcher, fromField, "from");
      ensureJoinFieldExistsAndHasDocValues(toSearcher, toField, "to");

      final SortedSetDocValues fromValues = DocValues.getSortedSet(fromSearcher.getSlowAtomicReader(), fromField);
      final SortedSetDocValues toValues = DocValues.getSortedSet(toSearcher.getSlowAtomicReader(), toField);
      ensureDocValuesAreNonEmpty(fromValues, fromField, "from");
      ensureDocValuesAreNonEmpty(toValues, toField, "to");
      final LongBitSet fromOrdBitSet = new LongBitSet(fromValues.getValueCount());
      final LongBitSet toOrdBitSet = new LongBitSet(toValues.getValueCount());

      final TermOrdinalCollector collector = new TermOrdinalCollector(fromField, fromValues, fromOrdBitSet);
      fromSearcher.search(q, collector);

      long fromOrdinal = 0;
      while ((fromOrdinal = fromOrdBitSet.nextSetBit(fromOrdinal)) > 0) {
        final BytesRef fromBytesRef = fromValues.lookupOrd(fromOrdinal);
        final long toOrdinal = toValues.lookupTerm(fromBytesRef);
        if (toOrdinal >= 0) {
          toOrdBitSet.set(toOrdinal);
        }
        fromOrdinal++;
      }
      return new JoinQueryCollector(toValues, toOrdBitSet);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean getCache() {
    return cache;
  }

  @Override
  public void setCache(boolean cache) {
    this.cache = cache;
  }

  @Override
  public int getCost() {
    return cost;
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
  
  private void ensureJoinFieldExistsAndHasDocValues(SolrIndexSearcher solrSearcher, String fieldName, String querySide) {
    final IndexSchema schema = solrSearcher.getSchema();
    final SchemaField field = schema.getFieldOrNull(fieldName);
    if (field == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, querySide + " field '" + fieldName + "' does not exist");
    }

    if (! field.hasDocValues()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Postfilter join queries require 'to' and 'from' fields to have docvalues enabled: '" +
              querySide + "' field '" + fieldName + "' doesn't");
    }
  }

  private void ensureDocValuesAreNonEmpty(SortedSetDocValues docValues, String fieldName, String type) {
    if (docValues.getValueCount() == 0) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "'" + type + "' field " + fieldName+ " has no docvalues");
    }
  }

  // Copied verbatim from JoinQParserPlugin.JoinQueryWeight ctor
  private void initializeSearchers(SolrIndexSearcher searcher) {
    SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
    if (info != null) {
      rb = info.getResponseBuilder();
    }

    if (fromIndex == null) {
      this.fromSearcher = searcher;
    } else {
      if (info == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cross-core join must have SolrRequestInfo");
      }

      CoreContainer container = searcher.getCore().getCoreContainer();
      final SolrCore fromCore = container.getCore(fromIndex);

      if (fromCore == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cross-core join: no such core " + fromIndex);
      }

      if (info.getReq().getCore() == fromCore) {
        // if this is the same core, use the searcher passed in... otherwise we could be warming and
        // get an older searcher from the core.
        fromSearcher = searcher;
      } else {
        // This could block if there is a static warming query with a join in it, and if useColdSearcher is true.
        // Deadlock could result if two cores both had useColdSearcher and had joins that used eachother.
        // This would be very predictable though (should happen every time if misconfigured)
        fromRef = fromCore.getSearcher(false, true, null);

        // be careful not to do anything with this searcher that requires the thread local
        // SolrRequestInfo in a manner that requires the core in the request to match
        fromSearcher = fromRef.get();
      }

      if (fromRef != null) {
        final RefCounted<SolrIndexSearcher> ref = fromRef;
        info.addCloseHook(new Closeable() {
          @Override
          public void close() {
            ref.decref();
          }
        });
      }

      info.addCloseHook(new Closeable() {
        @Override
        public void close() {
          fromCore.close();
        }
      });

    }
    this.toSearcher = searcher;
  }

  private static class TermOrdinalCollector extends DelegatingCollector {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private int docBase;
    private SortedSetDocValues topLevelDocValues;
    private final String fieldName;
    private final LongBitSet topLevelDocValuesBitSet;

    public TermOrdinalCollector(String fieldName, SortedSetDocValues topLevelDocValues, LongBitSet topLevelDocValuesBitSet) {
      this.fieldName = fieldName;
      this.topLevelDocValues = topLevelDocValues;
      this.topLevelDocValuesBitSet = topLevelDocValuesBitSet;
    }

    @Override
    public ScoreMode scoreMode() {
      return ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public void doSetNextReader(LeafReaderContext context) throws IOException {
      this.docBase = context.docBase;
    }

    @Override
    public void collect(int doc) throws IOException {
      final int globalDoc = docBase + doc;

      if (topLevelDocValues.advanceExact(globalDoc)) { // TODO The use of advanceExact assumes collect() is called in increasing docId order.  Is that true?
        long fieldValueOrd;
        while (SortedSetDocValues.NO_MORE_ORDS != (fieldValueOrd = topLevelDocValues.nextOrd())) {
          topLevelDocValuesBitSet.set(fieldValueOrd);
        }
      }
    }
  }

  private static class JoinQueryCollector extends DelegatingCollector {
    private LeafCollector leafCollector;
    private int docBase;
    private SortedSetDocValues topLevelDocValues;
    private LongBitSet topLevelDocValuesBitSet;

    public JoinQueryCollector(SortedSetDocValues topLevelDocValues, LongBitSet topLevelDocValuesBitSet) {
      this.topLevelDocValues = topLevelDocValues;
      this.topLevelDocValuesBitSet = topLevelDocValuesBitSet;
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
      leafCollector.setScorer(scorer);
    }

    @Override
    public void doSetNextReader(LeafReaderContext context) throws IOException {
      this.leafCollector = delegate.getLeafCollector(context);
      this.docBase = context.docBase;
    }

    @Override
    public void collect(int doc) throws IOException {
      final int globalDoc = doc + docBase;

      if (topLevelDocValues.advanceExact(globalDoc)) {
        while (true) {
          final long ord = topLevelDocValues.nextOrd();
          if (ord == SortedSetDocValues.NO_MORE_ORDS) break;
          if (topLevelDocValuesBitSet.get(ord)) {
            leafCollector.collect(doc);
            break;
          }
        }
      }
    }
  }
}
