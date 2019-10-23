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
import java.util.Arrays;
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PrefixCodedTerms;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocValuesTermsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.LongBitSet;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.PointField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Finds documents whose specified field has any of the specified values. It's like
 * {@link TermQParserPlugin} but multi-valued, and supports a variety of internal algorithms.
 * <br>Parameters:
 * <br><code>f</code>: The field name (mandatory)
 * <br><code>separator</code>: the separator delimiting the values in the query string, defaulting to a comma.
 * If it's a " " then it splits on any consecutive whitespace.
 * <br><code>method</code>: Any of termsFilter (default), booleanQuery, automaton, docValuesTermsFilter.
 * <p>
 * Note that if no values are specified then the query matches no documents.
 */
public class TermsQParserPlugin extends QParserPlugin {
  public static final String NAME = "terms";

  /** The separator to use in the underlying suggester */
  public static final String SEPARATOR = "separator";

  /** Choose the internal algorithm */
  private static final String METHOD = "method";

  private static enum Method {
    termsFilter {
      @Override
      Query makeFilter(String fname, BytesRef[] bytesRefs) {
        return new TermInSetQuery(fname, bytesRefs);// constant scores
      }
    },
    booleanQuery {
      @Override
      Query makeFilter(String fname, BytesRef[] byteRefs) {
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        for (BytesRef byteRef : byteRefs) {
          bq.add(new TermQuery(new Term(fname, byteRef)), BooleanClause.Occur.SHOULD);
        }
        return new ConstantScoreQuery(bq.build());
      }
    },
    automaton {
      @Override
      Query makeFilter(String fname, BytesRef[] byteRefs) {
        Automaton union = Automata.makeStringUnion(Arrays.asList(byteRefs));
        return new AutomatonQuery(new Term(fname), union);//constant scores
      }
    },
    docValuesTermsFilter {//on 4x this is FieldCacheTermsFilter but we use the 5x name any way
      @Override
      Query makeFilter(String fname, BytesRef[] byteRefs) {
        return new PostFilterDocValuesTermsQuery(fname, byteRefs);//constant scores
      }
    },
    docValuesTermsFilterPerSegment {
      @Override
      Query makeFilter(String fname, BytesRef[] byteRefs) {
        return new PerSegmentPostFilterDocValuesTermsQuery(fname, byteRefs);
      }
    };

    abstract Query makeFilter(String fname, BytesRef[] byteRefs);
  }

  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new QParser(qstr, localParams, params, req) {
      @Override
      public Query parse() throws SyntaxError {
        String fname = localParams.get(QueryParsing.F);
        FieldType ft = req.getSchema().getFieldTypeNoEx(fname);
        String separator = localParams.get(SEPARATOR, ",");
        String qstr = localParams.get(QueryParsing.V);//never null
        Method method = Method.valueOf(localParams.get(METHOD, Method.termsFilter.name()));
        //TODO pick the default method based on various heuristics from benchmarks
        //TODO pick the default using FieldType.getSetQuery

        //if space then split on all whitespace & trim, otherwise strictly interpret
        final boolean sepIsSpace = separator.equals(" ");
        if (sepIsSpace)
          qstr = qstr.trim();
        if (qstr.length() == 0)
          return new MatchNoDocsQuery();
        final String[] splitVals = sepIsSpace ? qstr.split("\\s+") : qstr.split(Pattern.quote(separator), -1);
        assert splitVals.length > 0;
        
        if (ft.isPointField()) {
          if (localParams.get(METHOD) != null) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Method '%s' not supported in TermsQParser when using PointFields", localParams.get(METHOD)));
          }
          return ((PointField)ft).getSetQuery(this, req.getSchema().getField(fname), Arrays.asList(splitVals));
        }

        BytesRef[] bytesRefs = new BytesRef[splitVals.length];
        BytesRefBuilder term = new BytesRefBuilder();
        for (int i = 0; i < splitVals.length; i++) {
          String stringVal = splitVals[i];
          //logic same as TermQParserPlugin
          if (ft != null) {
            ft.readableToIndexed(stringVal, term);
          } else {
            term.copyChars(stringVal);
          }
          bytesRefs[i] = term.toBytesRef();
        }

        return method.makeFilter(fname, bytesRefs);
      }
    };
  }

  private static class PostFilterDocValuesTermsQuery extends DocValuesTermsQuery implements PostFilter {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private String fieldName;
    private boolean cache = true;
    private boolean cacheSeparately = false;
    private int cost;


    public PostFilterDocValuesTermsQuery(String field, BytesRef... terms) {
      super(field, terms);
      System.out.println("Post filter!!!");
      this.fieldName = field;
    }

    @Override
    public DelegatingCollector getFilterCollector(IndexSearcher searcher) {
      System.out.println("Running the post filter!!!");
      try {
        long start = System.currentTimeMillis();
        final SortedSetDocValues docValues = DocValues.getSortedSet(((SolrIndexSearcher)searcher).getSlowAtomicReader(), fieldName);
        final LongBitSet topLevelDocValuesBitSet = new LongBitSet(docValues.getValueCount());
        boolean matchesAtLeastOneTerm = false;
        PrefixCodedTerms.TermIterator iterator = termData.iterator();
        long lastOrdFound = 0;
        long smallestOrd = -1;
        for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
          final long ord = lookupTerm(docValues, term, lastOrdFound);
          if (ord >= 0) {
            matchesAtLeastOneTerm = true;
            topLevelDocValuesBitSet.set(ord);
            if (smallestOrd == -1) smallestOrd = ord;
            lastOrdFound = ord;
          }
        }

        long end = System.currentTimeMillis();
        System.out.println("Lookups:"+Long.toString(end-start));

        if (matchesAtLeastOneTerm) {
          return new TermsCollector(fieldName, docValues, topLevelDocValuesBitSet, smallestOrd, lastOrdFound);
        } else {
          return new TermsCollector(fieldName, docValues, null, smallestOrd, lastOrdFound);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    /*
     * Same binary-search based implementation as SortedSetDocValues.lookupTerm(BytesRef), but with an
     * optimization to narrow the search space where possible by providing a startOrd instead of begining each search
     * at 0.
     */
    private long lookupTerm(SortedSetDocValues docValues, BytesRef key, long startOrd) throws IOException {
      long low = startOrd;
      long high = docValues.getValueCount()-1;

      while (low <= high) {
        long mid = (low + high) >>> 1;
        final BytesRef term = docValues.lookupOrd(mid);
        int cmp = term.compareTo(key);

        if (cmp < 0) {
          low = mid + 1;
        } else if (cmp > 0) {
          high = mid - 1;
        } else {
          return mid; // key found
        }
      }

      return -(low + 1);  // key not found.
    }

    @Override
    public void setCache(boolean cache) { this.cache = cache; }

    @Override
    public boolean getCache() {
      return (getCost() > 99) ? false : cache;
    }

    @Override
    public int getCost() { return cost; }

    @Override
    public void setCost(int cost) { this.cost = cost; }

    @Override
    public boolean getCacheSep() { return cacheSeparately; }

    @Override
    public void setCacheSep(boolean cacheSep) { this.cacheSeparately = cacheSep; }
  }

  private static class TermsCollector extends DelegatingCollector {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private int docBase;
    private final long firstOrd;
    private final long lastOrd;
    private SortedSetDocValues topLevelDocValues;
    private LongBitSet topLevelDocValueBitSet;

    /**
     *
     * @param fieldName the name of the field collected over
     * @param topLevelDocValues a top-level DocValues object
     * @param topLevelDocValueBitSet a doc-values bitset where only ordinals matching terms in the query are set
     */
    public TermsCollector(String fieldName, SortedSetDocValues topLevelDocValues, LongBitSet topLevelDocValueBitSet, long firstOrd, long lastOrd) {
      super();

      this.topLevelDocValues = topLevelDocValues;
      this.topLevelDocValueBitSet = topLevelDocValueBitSet;
      this.firstOrd = firstOrd;
      this.lastOrd = lastOrd;
    }

    private LeafCollector leafCollector;

    public void setScorer(Scorable scorer) throws IOException {
      leafCollector.setScorer(scorer);
    }

    public void doSetNextReader(LeafReaderContext context) throws IOException {
      this.leafCollector = delegate.getLeafCollector(context);
      this.docBase = context.docBase;
    }

    public void collect(int doc) throws IOException {
      if (topLevelDocValueBitSet == null) return; // Collect nothing if no terms matched doc-values entries
      final int globalDoc = doc + docBase;

      if (topLevelDocValues.advanceExact(globalDoc)) {
        while (true) {
          final long ord = topLevelDocValues.nextOrd();
          if (ord == SortedSetDocValues.NO_MORE_ORDS) break;
          if (ord < firstOrd || ord > lastOrd) continue;
          if (topLevelDocValueBitSet.get(ord)) {
            leafCollector.collect(doc);
            break;
          }
        }
      }
    }
  }

  private static class PerSegmentPostFilterDocValuesTermsQuery extends DocValuesTermsQuery implements PostFilter {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private String fieldName;
    private boolean cache = true;
    private boolean cacheSeparately = false;
    private int cost;


    public PerSegmentPostFilterDocValuesTermsQuery(String field, BytesRef... terms) {
      super(field, terms);
      System.out.println("Post filter!!!");
      this.fieldName = field;
    }

    @Override
    public DelegatingCollector getFilterCollector(IndexSearcher searcher) {
      System.out.println("Running the post filter!!!");
      return new PerSegmentTermsCollector(fieldName, termData);
    }

    @Override
    public void setCache(boolean cache) { this.cache = cache; }

    @Override
    public boolean getCache() {
      return cache;
    }

    @Override
    public int getCost() { return cost; }

    @Override
    public void setCost(int cost) { this.cost = cost; }

    @Override
    public boolean getCacheSep() { return cacheSeparately; }

    @Override
    public void setCacheSep(boolean cacheSep) { this.cacheSeparately = cacheSep; }
  }

  private static class PerSegmentTermsCollector extends DelegatingCollector {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private int currentDocValuesPosition = -1;
    private SortedSetDocValues currentSegmentDocValues;
    private LongBitSet currentSegmentBitSet;
    private final String fieldName;
    private PrefixCodedTerms termData;


    public PerSegmentTermsCollector(String fieldName, PrefixCodedTerms termData) {
      super();

      this.fieldName = fieldName;
      this.termData = termData;
    }

    private LeafCollector leafCollector;

    public void setScorer(Scorable scorer) throws IOException {
      leafCollector.setScorer(scorer);
    }

    public void doSetNextReader(LeafReaderContext context) throws IOException {
      currentSegmentDocValues = context.reader().getSortedSetDocValues(fieldName);
      currentSegmentBitSet = new LongBitSet(currentSegmentDocValues.getValueCount());
      currentDocValuesPosition = -1;

      final PrefixCodedTerms.TermIterator iterator = termData.iterator();
      long lastOrdFound = 0;
      for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
        final long ord = lookupTerm(currentSegmentDocValues, term, lastOrdFound);
        if (ord >= 0) {
          currentSegmentBitSet.set(ord);
          lastOrdFound = ord;
        }
      }
      this.leafCollector = delegate.getLeafCollector(context);
    }

    public void collect(int doc) throws IOException {
      if (currentDocValuesPosition < doc) {
        currentDocValuesPosition = currentSegmentDocValues.advance(doc);
      }

      if (currentDocValuesPosition == doc) {
        while (true) {
          final long ord = currentSegmentDocValues.nextOrd();
          if (ord == SortedSetDocValues.NO_MORE_ORDS) break;
          if (currentSegmentBitSet.get(ord)) {
            leafCollector.collect(doc);
            break;
          }
        }
      }
    }

    /*
     * Same binary-search based implementation as SortedSetDocValues.lookupTerm(BytesRef), but with an
     * optimization to narrow the search space where possible by providing a startOrd instead of begining each search
     * at 0.
     */
    private static long lookupTerm(SortedSetDocValues docValues, BytesRef key, long startOrd) throws IOException {
      long low = startOrd;
      long high = docValues.getValueCount()-1;

      while (low <= high) {
        long mid = (low + high) >>> 1;
        final BytesRef term = docValues.lookupOrd(mid);
        int cmp = term.compareTo(key);

        if (cmp < 0) {
          low = mid + 1;
        } else if (cmp > 0) {
          high = mid - 1;
        } else {
          return mid; // key found
        }
      }

      return -(low + 1);  // key not found.
    }
  }
}
