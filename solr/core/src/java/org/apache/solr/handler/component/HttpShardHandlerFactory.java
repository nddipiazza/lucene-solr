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
package org.apache.solr.handler.component;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.LBHttpSolrClient;
import org.apache.solr.client.solrj.impl.LBHttpSolrClient.Builder;
import org.apache.solr.client.solrj.impl.PreferenceRule;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.URLUtil;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.update.UpdateShardHandlerConfig;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.solr.util.stats.HttpClientMetricNameStrategy;
import org.apache.solr.util.stats.InstrumentedHttpRequestExecutor;
import org.apache.solr.util.stats.InstrumentedPoolingHttpClientConnectionManager;
import org.apache.solr.util.stats.MetricUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.util.stats.InstrumentedHttpRequestExecutor.KNOWN_METRIC_NAME_STRATEGIES;


public class HttpShardHandlerFactory extends ShardHandlerFactory implements org.apache.solr.util.plugin.PluginInfoInitialized, SolrMetricProducer {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String DEFAULT_SCHEME = "http";
  
  // We want an executor that doesn't take up any resources if
  // it's not used, so it could be created statically for
  // the distributed search component if desired.
  //
  // Consider CallerRuns policy and a lower max threads to throttle
  // requests at some point (or should we simply return failure?)
  private ExecutorService commExecutor = new ExecutorUtil.MDCAwareThreadPoolExecutor(
      0,
      Integer.MAX_VALUE,
      5, TimeUnit.SECONDS, // terminate idle threads after 5 sec
      new SynchronousQueue<>(),  // directly hand off tasks
      new DefaultSolrThreadFactory("httpShardExecutor"),
      // the Runnable added to this executor handles all exceptions so we disable stack trace collection as an optimization
      // see SOLR-11880 for more details
      false
  );

  protected InstrumentedPoolingHttpClientConnectionManager clientConnectionManager;
  protected CloseableHttpClient defaultClient;
  protected InstrumentedHttpRequestExecutor httpRequestExecutor;
  private LBHttpSolrClient loadbalancer;
  //default values:
  int soTimeout = HttpClientUtil.DEFAULT_SO_TIMEOUT;
  int connectionTimeout = HttpClientUtil.DEFAULT_CONNECT_TIMEOUT;
  int maxConnectionsPerHost = HttpClientUtil.DEFAULT_MAXCONNECTIONSPERHOST;
  int maxConnections = HttpClientUtil.DEFAULT_MAXCONNECTIONS;
  int corePoolSize = 0;
  int maximumPoolSize = Integer.MAX_VALUE;
  int keepAliveTime = 5;
  int queueSize = -1;
  int   permittedLoadBalancerRequestsMinimumAbsolute = 0;
  float permittedLoadBalancerRequestsMaximumFraction = 1.0f;
  boolean accessPolicy = false;

  private String scheme = null;

  private HttpClientMetricNameStrategy metricNameStrategy;

  private String metricTag;

  protected final Random r = new Random();

  private final ReplicaListTransformer shufflingReplicaListTransformer = new ShufflingReplicaListTransformer(r);

  // URL scheme to be used in distributed search.
  static final String INIT_URL_SCHEME = "urlScheme";

  // The core size of the threadpool servicing requests
  static final String INIT_CORE_POOL_SIZE = "corePoolSize";

  // The maximum size of the threadpool servicing requests
  static final String INIT_MAX_POOL_SIZE = "maximumPoolSize";

  // The amount of time idle threads persist for in the queue, before being killed
  static final String MAX_THREAD_IDLE_TIME = "maxThreadIdleTime";

  // If the threadpool uses a backing queue, what is its maximum size (-1) to use direct handoff
  static final String INIT_SIZE_OF_QUEUE = "sizeOfQueue";

  // The minimum number of replicas that may be used
  static final String LOAD_BALANCER_REQUESTS_MIN_ABSOLUTE = "loadBalancerRequestsMinimumAbsolute";

  // The maximum proportion of replicas to be used
  static final String LOAD_BALANCER_REQUESTS_MAX_FRACTION = "loadBalancerRequestsMaximumFraction";

  // Configure if the threadpool favours fairness over throughput
  static final String INIT_FAIRNESS_POLICY = "fairnessPolicy";

  /**
   * Get {@link ShardHandler} that uses the default http client.
   */
  @Override
  public ShardHandler getShardHandler() {
    return getShardHandler(defaultClient);
  }

  /**
   * Get {@link ShardHandler} that uses custom http client.
   */
  public ShardHandler getShardHandler(final HttpClient httpClient){
    return new HttpShardHandler(this, httpClient);
  }

  private static NamedList<?> getNamedList(Object val) {
    if (val instanceof NamedList) {
      return (NamedList<?>)val;
    } else {
      throw new IllegalArgumentException("Invalid config for replicaRouting; expected NamedList, but got " + val);
    }
  }

  private static String checkDefaultReplicaListTransformer(NamedList<?> c, String setTo, String extantDefaultRouting) {
    if (!Boolean.TRUE.equals(c.getBooleanArg("default"))) {
      return null;
    } else {
      if (extantDefaultRouting == null) {
        return setTo;
      } else {
        throw new IllegalArgumentException("more than one routing scheme marked as default");
      }
    }
  }

  private void initReplicaListTransformers(NamedList routingConfig) {
    String defaultRouting = null;
    if (routingConfig != null && routingConfig.size() > 0) {
      Iterator<Map.Entry<String,?>> iter = routingConfig.iterator();
      do {
        Map.Entry<String, ?> e = iter.next();
        String key = e.getKey();
        switch (key) {
          case ShardParams.REPLICA_RANDOM:
            // Only positive assertion of default status (i.e., default=true) is supported.
            // "random" is currently the implicit default, so explicitly configuring
            // "random" as default would not currently be useful, but if the implicit default
            // changes in the future, checkDefault could be relevant here.
            defaultRouting = checkDefaultReplicaListTransformer(getNamedList(e.getValue()), key, defaultRouting);
            break;
          case ShardParams.REPLICA_STABLE:
            NamedList<?> c = getNamedList(e.getValue());
            defaultRouting = checkDefaultReplicaListTransformer(c, key, defaultRouting);
            this.stableRltFactory = new AffinityReplicaListTransformerFactory(c);
            break;
          default:
            throw new IllegalArgumentException("invalid replica routing spec name: " + key);
        }
      } while (iter.hasNext());
    }
    if (this.stableRltFactory == null) {
      this.stableRltFactory = new AffinityReplicaListTransformerFactory();
    }
    if (ShardParams.REPLICA_STABLE.equals(defaultRouting)) {
      this.defaultRltFactory = this.stableRltFactory;
    } else {
      this.defaultRltFactory = this.randomRltFactory;
    }
  }

  @Override
  public void init(PluginInfo info) {
    StringBuilder sb = new StringBuilder();
    NamedList args = info.initArgs;
    this.soTimeout = getParameter(args, HttpClientUtil.PROP_SO_TIMEOUT, soTimeout,sb);
    this.scheme = getParameter(args, INIT_URL_SCHEME, null,sb);
    if(StringUtils.endsWith(this.scheme, "://")) {
      this.scheme = StringUtils.removeEnd(this.scheme, "://");
    }

    String strategy = getParameter(args, "metricNameStrategy", UpdateShardHandlerConfig.DEFAULT_METRICNAMESTRATEGY, sb);
    this.metricNameStrategy = KNOWN_METRIC_NAME_STRATEGIES.get(strategy);
    if (this.metricNameStrategy == null)  {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Unknown metricNameStrategy: " + strategy + " found. Must be one of: " + KNOWN_METRIC_NAME_STRATEGIES.keySet());
    }

    this.connectionTimeout = getParameter(args, HttpClientUtil.PROP_CONNECTION_TIMEOUT, connectionTimeout, sb);
    this.maxConnectionsPerHost = getParameter(args, HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, maxConnectionsPerHost,sb);
    this.maxConnections = getParameter(args, HttpClientUtil.PROP_MAX_CONNECTIONS, maxConnections,sb);
    this.corePoolSize = getParameter(args, INIT_CORE_POOL_SIZE, corePoolSize,sb);
    this.maximumPoolSize = getParameter(args, INIT_MAX_POOL_SIZE, maximumPoolSize,sb);
    this.keepAliveTime = getParameter(args, MAX_THREAD_IDLE_TIME, keepAliveTime,sb);
    this.queueSize = getParameter(args, INIT_SIZE_OF_QUEUE, queueSize,sb);
    this.permittedLoadBalancerRequestsMinimumAbsolute = getParameter(
        args,
        LOAD_BALANCER_REQUESTS_MIN_ABSOLUTE,
        permittedLoadBalancerRequestsMinimumAbsolute,
        sb);
    this.permittedLoadBalancerRequestsMaximumFraction = getParameter(
        args,
        LOAD_BALANCER_REQUESTS_MAX_FRACTION,
        permittedLoadBalancerRequestsMaximumFraction,
        sb);
    this.accessPolicy = getParameter(args, INIT_FAIRNESS_POLICY, accessPolicy,sb);

    // magic sysprop to make tests reproducible: set by SolrTestCaseJ4.
    String v = System.getProperty("tests.shardhandler.randomSeed");
    if (v != null) {
      r.setSeed(Long.parseLong(v));
    }

    BlockingQueue<Runnable> blockingQueue = (this.queueSize == -1) ?
        new SynchronousQueue<Runnable>(this.accessPolicy) :
        new ArrayBlockingQueue<Runnable>(this.queueSize, this.accessPolicy);

    this.commExecutor = new ExecutorUtil.MDCAwareThreadPoolExecutor(
        this.corePoolSize,
        this.maximumPoolSize,
        this.keepAliveTime, TimeUnit.SECONDS,
        blockingQueue,
        new DefaultSolrThreadFactory("httpShardExecutor")
    );

    ModifiableSolrParams clientParams = getClientParams();
    httpRequestExecutor = new InstrumentedHttpRequestExecutor(this.metricNameStrategy);
    clientConnectionManager = new InstrumentedPoolingHttpClientConnectionManager(HttpClientUtil.getSchemaRegisteryProvider().getSchemaRegistry());
    this.defaultClient = HttpClientUtil.createClient(clientParams, clientConnectionManager, false, httpRequestExecutor);
    this.loadbalancer = createLoadbalancer(defaultClient);

    initReplicaListTransformers(getParameter(args, "replicaRouting", null, sb));

    log.debug("created with {}",sb);
  }

  protected ModifiableSolrParams getClientParams() {
    ModifiableSolrParams clientParams = new ModifiableSolrParams();
    clientParams.set(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, maxConnectionsPerHost);
    clientParams.set(HttpClientUtil.PROP_MAX_CONNECTIONS, maxConnections);
    return clientParams;
  }

  protected ExecutorService getThreadPoolExecutor(){
    return this.commExecutor;
  }

  protected LBHttpSolrClient createLoadbalancer(HttpClient httpClient){
    LBHttpSolrClient client = new Builder()
        .withHttpClient(httpClient)
        .withConnectionTimeout(connectionTimeout)
        .withSocketTimeout(soTimeout)
        .build();
    return client;
  }

  protected <T> T getParameter(NamedList initArgs, String configKey, T defaultValue, StringBuilder sb) {
    T toReturn = defaultValue;
    if (initArgs != null) {
      T temp = (T) initArgs.get(configKey);
      toReturn = (temp != null) ? temp : defaultValue;
    }
    if(sb!=null && toReturn != null) sb.append(configKey).append(" : ").append(toReturn).append(",");
    return toReturn;
  }


  @Override
  public void close() {
    try {
      ExecutorUtil.shutdownAndAwaitTermination(commExecutor);
    } finally {
      try {
        if (loadbalancer != null) {
          loadbalancer.close();
        }
      } finally { 
        if (defaultClient != null) {
          HttpClientUtil.close(defaultClient);
        }
        if (clientConnectionManager != null)  {
          clientConnectionManager.close();
        }
      }
    }
  }

  /**
   * Makes a request to one or more of the given urls, using the configured load balancer.
   *
   * @param req The solr search request that should be sent through the load balancer
   * @param urls The list of solr server urls to load balance across
   * @return The response from the request
   */
  public LBHttpSolrClient.Rsp makeLoadBalancedRequest(final QueryRequest req, List<String> urls)
    throws SolrServerException, IOException {
    return loadbalancer.request(newLBHttpSolrClientReq(req, urls));
  }

  protected LBHttpSolrClient.Req newLBHttpSolrClientReq(final QueryRequest req, List<String> urls) {
    int numServersToTry = (int)Math.floor(urls.size() * this.permittedLoadBalancerRequestsMaximumFraction);
    if (numServersToTry < this.permittedLoadBalancerRequestsMinimumAbsolute) {
      numServersToTry = this.permittedLoadBalancerRequestsMinimumAbsolute;
    }
    return new LBHttpSolrClient.Req(req, urls, numServersToTry);
  }

  /**
   * Creates a list of urls for the given shard.
   *
   * @param shard the urls for the shard, separated by '|'
   * @return A list of valid urls (including protocol) that are replicas for the shard
   */
  public List<String> buildURLList(String shard) {
    List<String> urls = StrUtils.splitSmart(shard, "|", true);

    // convert shard to URL
    for (int i=0; i<urls.size(); i++) {
      urls.set(i, buildUrl(urls.get(i)));
    }

    return urls;
  }

  /**
   * A distributed request is made via {@link LBHttpSolrClient} to the first live server in the URL list.
   * This means it is just as likely to choose current host as any of the other hosts.
   * This function makes sure that the cores are sorted according to the given list of preferences.
   * E.g. If all nodes prefer local cores then a bad/heavily-loaded node will receive less requests from
   * healthy nodes. This will help prevent a distributed deadlock or timeouts in all the healthy nodes due
   * to one bad node.
   *
   * Optional final preferenceRule is *not* used for pairwise sorting, but instead defines how "equivalent"
   * replicas will be ordered (the base ordering). Defaults to "random"; may specify "stable".
   */
  static class NodePreferenceRulesComparator implements Comparator<Object> {

    private final SolrQueryRequest request;
    private final List<PreferenceRule> sortRules;
    private final List<PreferenceRule> preferenceRules;
    private String localHostAddress = null;
    private final ReplicaListTransformer baseReplicaListTransformer;

    public NodePreferenceRulesComparator(final List<PreferenceRule> preferenceRules, final SolrQueryRequest request,
                                         final ReplicaListTransformerFactory defaultRltFactory, final ReplicaListTransformerFactory randomRltFactory,
                                         final ReplicaListTransformerFactory stableRltFactory) {
      this.request = request;
      final SolrCore core; // explicit check for null core (temporary?, for tests)
      this.preferenceRules = preferenceRules;
      final int maxIdx = preferenceRules.size() - 1;
      final PreferenceRule lastRule = preferenceRules.get(maxIdx);
      if (!ShardParams.SHARDS_PREFERENCE_REPLICA_BASE.equals(lastRule.name)) {
        this.sortRules = preferenceRules;
        this.baseReplicaListTransformer = defaultRltFactory.getInstance(null, request, randomRltFactory);
      } else {
        if (maxIdx == 0) {
          this.sortRules = null;
        } else {
          this.sortRules = preferenceRules.subList(0, maxIdx);
        }
        String[] parts = lastRule.value.split(":", 2);
        switch (parts[0]) {
          case ShardParams.REPLICA_RANDOM:
            this.baseReplicaListTransformer = randomRltFactory.getInstance(parts.length == 1 ? null : parts[1], request, null);
            break;
          case ShardParams.REPLICA_STABLE:
            this.baseReplicaListTransformer = stableRltFactory.getInstance(parts.length == 1 ? null : parts[1], request, randomRltFactory);
            break;
          default:
            throw new IllegalArgumentException("Invalid base replica order spec");
        }
      }
    }
    private static final ReplicaListTransformer NOOP_RLT = (List<?> choices) -> { /* noop */ };
    private static final ReplicaListTransformerFactory NOOP_RLTF = (String configSpec, SolrQueryRequest request,
                                                                    ReplicaListTransformerFactory fallback) -> NOOP_RLT;
    /**
     * For compatibility with tests, which expect this constructor to have no effect on the *base* order.
     */
    NodePreferenceRulesComparator(final List<PreferenceRule> sortRules, final SolrQueryRequest request) {
      this(sortRules, request, NOOP_RLTF, null, null);
    }

    public ReplicaListTransformer getBaseReplicaListTransformer() {
      return baseReplicaListTransformer;
    }

    @Override
    public int compare(Object left, Object right) {
      if (this.sortRules != null) {
        for (PreferenceRule preferenceRule: this.sortRules) {
          final boolean lhs;
          final boolean rhs;
          switch (preferenceRule.name) {
            case ShardParams.SHARDS_PREFERENCE_REPLICA_TYPE:
              lhs = hasReplicaType(left, preferenceRule.value);
              rhs = hasReplicaType(right, preferenceRule.value);
              break;
            case ShardParams.SHARDS_PREFERENCE_REPLICA_LOCATION:
              lhs = hasCoreUrlPrefix(left, preferenceRule.value);
              rhs = hasCoreUrlPrefix(right, preferenceRule.value);
              break;
            case ShardParams.SHARDS_PREFERENCE_REPLICA_BASE:
              throw new IllegalArgumentException("only one base replica order may be specified in "
                  + ShardParams.SHARDS_PREFERENCE + ", and it must be specified last");
            default:
              throw new IllegalArgumentException("Invalid " + ShardParams.SHARDS_PREFERENCE + " type: " + preferenceRule.name);
          }
          if (lhs != rhs) {
            return lhs ? -1 : +1;
          }
        }
      }
      return 0;
    }

    private boolean hasCoreUrlPrefix(Object o, String prefix) {
      final String s;
      if (o instanceof String) {
        s = (String)o;
      }
      else if (o instanceof Replica) {
        s = ((Replica)o).getCoreUrl();
      } else {
        return false;
      }
      if (prefix.equals(ShardParams.REPLICA_LOCAL)) {
        if (null == localHostAddress) {
          final ZkController zkController = this.request.getCore().getCoreContainer().getZkController();
          localHostAddress = zkController != null ? zkController.getBaseUrl() : "";
          if (localHostAddress.isEmpty()) {
            log.warn("Couldn't determine current host address for sorting of local replicas");
          }
        }
        if (!localHostAddress.isEmpty()) {
          if (s.startsWith(localHostAddress)) {
            return true;
          }
        }
      } else {
        if (s.startsWith(prefix)) {
          return true;
        }
      }
      return false;
    }
    private static boolean hasReplicaType(Object o, String preferred) {
      if (!(o instanceof Replica)) {
        return false;
      }
      final String s = ((Replica)o).getType().toString();
      return s.equals(preferred);
    }
  }

  private final ReplicaListTransformerFactory randomRltFactory = (String configSpec, SolrQueryRequest request,
                                                                  ReplicaListTransformerFactory fallback) -> shufflingReplicaListTransformer;
  private ReplicaListTransformerFactory stableRltFactory;
  private ReplicaListTransformerFactory defaultRltFactory;

  /**
   * Private class responsible for applying pairwise sort based on inherent replica attributes,
   * and subsequently reordering any equivalent replica sets according to behavior specified
   * by the baseReplicaListTransformer.
   */
  private static final class TopLevelReplicaListTransformer implements ReplicaListTransformer {

    private final NodePreferenceRulesComparator replicaComp;
    private final ReplicaListTransformer baseReplicaListTransformer;

    public TopLevelReplicaListTransformer(NodePreferenceRulesComparator replicaComp, ReplicaListTransformer baseReplicaListTransformer) {
      this.replicaComp = replicaComp;
      this.baseReplicaListTransformer = baseReplicaListTransformer;
    }

    @Override
    public void transform(List<?> choices) {
      if (choices.size() > 1) {
        if (log.isDebugEnabled()) {
          log.debug("Applying the following sorting preferences to replicas: {}",
              Arrays.toString(replicaComp.preferenceRules.toArray()));
        }

        // First, sort according to comparator rules.
        try {
          choices.sort(replicaComp);
        } catch (IllegalArgumentException iae) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              iae.getMessage()
          );
        }

        // Next determine all boundaries between replicas ranked as "equivalent" by the comparator
        Iterator<?> iter = choices.iterator();
        Object prev = iter.next();
        Object current;
        int idx = 1;
        int boundaryCount = 0;
        int[] boundaries = new int[choices.size() - 1];
        do {
          current = iter.next();
          if (replicaComp.compare(prev, current) != 0) {
            boundaries[boundaryCount++] = idx;
          }
          prev = current;
          idx++;
        } while (iter.hasNext());

        // Finally inspect boundaries to apply base transformation, where necessary (separate phase to avoid ConcurrentModificationException)
        int startIdx = 0;
        int endIdx;
        for (int i = 0; i < boundaryCount; i++) {
          endIdx = boundaries[i];
          if (endIdx - startIdx > 1) {
            baseReplicaListTransformer.transform(choices.subList(startIdx, endIdx));
          }
          startIdx = endIdx;
        }

        if (log.isDebugEnabled()) {
          log.debug("Applied sorting preferences to replica list: {}",
              Arrays.toString(choices.toArray()));
        }
      }
    }
  }

  protected ReplicaListTransformer getReplicaListTransformer(final SolrQueryRequest req) {
    final SolrParams params = req.getParams();
    log.info("Getting replica list transformer for solr query request: {}", req);
    @SuppressWarnings("deprecation")
    final boolean preferLocalShards = params.getBool(CommonParams.PREFER_LOCAL_SHARDS, false);
    final String shardsPreferenceSpec = params.get(ShardParams.SHARDS_PREFERENCE, "");

    if (preferLocalShards || !shardsPreferenceSpec.isEmpty()) {
      if (preferLocalShards && !shardsPreferenceSpec.isEmpty()) {
        throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "preferLocalShards is deprecated and must not be used with shards.preference" 
        );
      }
      List<PreferenceRule> preferenceRules = PreferenceRule.from(shardsPreferenceSpec);
      if (preferLocalShards) {
        preferenceRules.add(new PreferenceRule(ShardParams.SHARDS_PREFERENCE_REPLICA_LOCATION, ShardParams.REPLICA_LOCAL));
      }
      NodePreferenceRulesComparator replicaComp = new NodePreferenceRulesComparator(preferenceRules, req,
          defaultRltFactory, randomRltFactory, stableRltFactory);
      ReplicaListTransformer baseReplicaListTransformer = replicaComp.getBaseReplicaListTransformer();
      if (replicaComp.sortRules == null) {
        // only applying base transformation
        log.info("Return base replica list transformer: {}", baseReplicaListTransformer);
        return baseReplicaListTransformer;
      } else {
        ReplicaListTransformer res = new TopLevelReplicaListTransformer(replicaComp, baseReplicaListTransformer);
        log.info("Return new top level replica list transformer replica list transformer: {}", res);
        return res;
      }
    }

    ReplicaListTransformer res = defaultRltFactory.getInstance(null, req, randomRltFactory);
    log.info("Return default RLT factory: {}", res);
    return res;
  }

  /**
   * Creates a new completion service for use by a single set of distributed requests.
   */
  public CompletionService newCompletionService() {
    return new ExecutorCompletionService<ShardResponse>(commExecutor);
  }
  
  /**
   * Rebuilds the URL replacing the URL scheme of the passed URL with the
   * configured scheme replacement.If no scheme was configured, the passed URL's
   * scheme is left alone.
   */
  private String buildUrl(String url) {
    if(!URLUtil.hasScheme(url)) {
      return StringUtils.defaultIfEmpty(scheme, DEFAULT_SCHEME) + "://" + url;
    } else if(StringUtils.isNotEmpty(scheme)) {
      return scheme + "://" + URLUtil.removeScheme(url);
    }
    
    return url;
  }

  @Override
  public void initializeMetrics(SolrMetricManager manager, String registry, String tag, String scope) {
    this.metricTag = tag;
    String expandedScope = SolrMetricManager.mkName(scope, SolrInfoBean.Category.QUERY.name());
    clientConnectionManager.initializeMetrics(manager, registry, tag, expandedScope);
    httpRequestExecutor.initializeMetrics(manager, registry, tag, expandedScope);
    commExecutor = MetricUtils.instrumentedExecutorService(commExecutor, null,
        manager.registry(registry),
        SolrMetricManager.mkName("httpShardExecutor", expandedScope, "threadPool"));
  }

}
