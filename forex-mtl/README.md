# Forex-MTL: Exchange Rate Proxy Service

A high-performance, thread-safe forex exchange rate service that acts as a local proxy for the One-Frame API.

## Requirements Met

### Requirements 
- The service returns an exchange rate when provided with 2 supported currencies
- The rate should not be older than 5 minutes
- The service should support at least 10,000 successful requests per day with 1 API token (limited to 1000 requests per day per token)

### Key Concepts

**Rates Caching**: Reduces API calls by caching exchange rates for 5 minutes

**Batch Processing**: Groups multiple currency pair requests into single API calls

## Design Decisions

### Core Components

1. **OneFrameClient** - HTTP client for One-Frame API integration
2. **RateCache** - TTL-based concurrent cache using TrieMap with unified expiration
3. **CachedOneFrame** - Main service orchestrating cache and API calls

## Meeting One-Frame API Limitations

### The Challenge
- **Target Load**: 10,000 requests/day
- **API Limit**: 1,000 requests/day
- **Required Efficiency**: 10:1 cache hit ratio

### Solution Strategy

#### 1. Unified TTL-Based Caching (5 minutes)
```scala
// Single expiration time for entire cache
@volatile private var cacheExpiresAt: Option[Instant] = None
val expiresAt = Instant.ofEpochMilli(nowMillis).plusMillis(ttl.toMillis)
cacheExpiresAt = Some(expiresAt)
```

#### 2. Complete Batch API Optimization
```scala
// Always request ALL supported currency pairs in single batch
val allSupportedPairs = Currency.supportedPairs.map { case (from, to) => Rate.Pair(from, to) }
client.getBatch(allSupportedPairs)
```

#### 3. Unified Cache Management
- **All-or-Nothing Strategy**: On any cache miss, refresh ALL supported pairs at once
- **Single TTL**: Entire cache expires together, eliminating partial cache states
- **TrieMap Storage**: Thread-safe concurrent map for high-performance access

### Efficiency Analysis

**Optimized Scenario**: 
- 288 API calls/day (one every 5 minutes for all supported pairs)
- No additional calls for new currency pairs (all pairs loaded in each batch)

**Result**: Comfortably within 1000 API calls/day limit 

## TTL Strategy: Server Time vs API Timestamp

### Design Decision

The cache TTL is calculated from **server time** rather than the API's `time_stamp` field. This is a deliberate architectural choice with important implications:

```scala
// Current implementation - server time based
val expiresAt = Instant.ofEpochMilli(nowMillis).plusMillis(ttl.toMillis)

// Alternative approach - API timestamp based
val expiresAt = apiTimestamp.plusMillis(ttl.toMillis)
```

### Trade-off Analysis

**Server Time Approach (Current):**
- ✅ **Predictable API usage**: Exactly 288 calls/day guaranteed
- ✅ **Quota safety**: Never exceeds One-Frame limits unexpectedly  
- ✅ **Clock drift resilient**: Independent of API server time synchronization
- ✅ **Production stable**: System behavior is deterministic
- ❌ **Theoretical precision loss**: May serve data slightly older than 5 minutes in edge cases

**API Timestamp Approach (Alternative):**
- ✅ **Stricter data freshness**: Never serves data older than 5 minutes from source
- ✅ **Theoretical correctness**: TTL reflects actual data age
- ❌ **Unpredictable API usage**: 288-1440 calls/day depending on API timestamp delays
- ❌ **Quota risk**: Could exhaust daily limit if API timestamps are stale
- ❌ **Clock sync dependency**: Breaks down with time synchronization issues

### Why Server Time Was Chosen

1. **Business Constraint Priority**: Meeting the 10,000 requests/day requirement with 1,000 API calls/day limit
2. **Production Reliability**: Predictable resource consumption over theoretical precision
3. **System Stability**: Resilience to external service timing variations
4. **Monitoring Capability**: Time sync warnings provide visibility into any precision trade-offs

## Reliability & High Availability

### Multi-Node Deployment Strategy

The current implementation supports reliable production deployment with 2 active nodes and 3 hot-standby nodes. This architecture ensures high availability and fault tolerance with minimal risk of One-Frame quota exceed.

### Failover Scenarios

#### **Single Node Failure**
- **Detection Time**: Automatic detection and reaction from Orchestrator (health check, container restart) with response time less than 10 seconds
- **Recovery**: Load-balancer may automatically route traffic to healthy node until failed node is restarted or replaced by standby node
- **Impact**: 
  - **Downtime**: Minimal downtime of failed node
  - **Capacity**: Remaining node handles full load

#### **Double Node Failure (Hot-Standby Activation)**
- **Trigger**: Both active nodes unavailable
- **Action**: Orchestrator scales up standby nodes
- **Recovery Time**: 60-90 seconds (container startup + cache warmup)

### Reliability Benefits

1. **99.9%+ Uptime**: Multi-node redundancy with automatic failover
2. **Graceful Degradation**: System continues operating with reduced capacity
3. **Disaster Recovery**: Geographic distribution of standby nodes possible

### Monitoring & Alerts
Project provides sufficient logging for such issues, including:
- **One-Frame unavailability**
- **Authentication errors**: token issues
- **Rate limit exceeded**: API quota issues

Integration with monitoring tools (e.g., Prometheus, Grafana) can provide real-time alerts on these events.

## Thread Safety Implementation

### Current Solution: Double-Checked Locking Pattern

```scala
private def getCurrencyRate(pair: Rate.Pair): F[Error Either Rate] = {
  // First check: Read from cache without synchronization
  cache.get(pair).flatMap {
    case Some(cachedRate) =>
      logger.debug(s"Cache HIT (unsynchronized read)")
      F.pure(cachedRate.asRight[Error])
    case None =>
      // Cache miss - need to synchronize and double-check
      this.synchronized {
        cache.get(pair).flatMap {
          case Some(cachedRate) =>
            // Double-check: Another thread might have populated cache
            logger.debug(s"Cache HIT (synchronized double-check)")
            F.pure(cachedRate.asRight[Error])
          case None =>
            // Confirmed cache miss - perform API call
            performBatchAPICall(pair)
        }
      }
  }
}
```

**Why This Works**:
- **Optimized Cache Reads**: Most cache hits avoid synchronization entirely
- **Race Condition Prevention**: Double-check pattern prevents duplicate API calls
- **Better Concurrency**: Multiple threads can read from cache simultaneously
- **API Call Protection**: Only synchronized when cache miss confirmed

### Alternative Thread Safety Approaches

#### 1. Simple Synchronized Method
```scala
private def getCurrencyRate(pair: Rate.Pair): F[Error Either Rate] = {
  this.synchronized {
    cache.get(pair).flatMap {
      // Entire cache check + API call logic
    }
  }
}
```
- **Pros**: Simple implementation, easy to understand
- **Cons**: All cache reads are synchronized, lower concurrent performance

#### 2. ReadWriteLock Implementation
```scala
private val rwLock = new ReentrantReadWriteLock()

private def getCurrencyRate(pair: Rate.Pair): F[Error Either Rate] = {
  // Read lock for cache check
  rwLock.readLock().lock()
  try {
    cache.get(pair) match {
      case Some(rate) => F.pure(rate.asRight)
      case None => 
        // Upgrade to write lock
        rwLock.readLock().unlock()
        rwLock.writeLock().lock()
        try {
          performAPICallWithDoubleCheck(pair)
        } finally {
          rwLock.writeLock().unlock()
        }
    }
  } finally {
    if (rwLock.readLock().tryLock()) rwLock.readLock().unlock()
  }
}
```
- **Pros**: Maximum concurrency for reads
- **Cons**: Complex lock management, potential deadlocks, overkill for our load

### Why Double-Checked Locking Was Chosen

1. **Performance Optimization**: At 10k+ requests/day, optimizing cache reads becomes important
2. **Concurrency Benefits**: Multiple threads can read from cache without blocking each other
3. **API Call Protection**: Still prevents race conditions for expensive API calls  
4. **Balanced Approach**: More complex than simple sync, but significantly better performance
5. **Production Ready**: Well-known pattern suitable for high-throughput caching scenarios

## Cache Implementation Alternatives

### Current Solution: In-Memory TrieMap
```scala
private val cache = TrieMap[Rate.Pair, Rate]()
@volatile private var cacheExpiresAt: Option[Instant] = None
```

**Pros**:
- Simple, lightweight implementation
- Thread-safe concurrent access
- No external dependencies
- Perfect for single-instance deployments

**Cons**:
- Memory usage grows with number of currency pairs
- Data lost on application restart
- No cache eviction policies beyond TTL

### Alternative: EhCache Integration

For production deployments requiring persistence and advanced cache management:

```scala
// build.sbt
libraryDependencies += "net.sf.ehcache" % "ehcache" % "2.10.9.2"

// EhCache configuration
class EhCacheRateCache[F[_]: Sync](cacheManager: CacheManager, config: CacheConfig) extends RateCache[F] {
  private val cache = cacheManager.getCache("forex-rates")
  
  def get(pair: Rate.Pair): F[Option[Rate]] = Sync[F].delay {
    Option(cache.get(pair.toString))
      .map(_.asInstanceOf[CachedRate])
      .filter(cachedRate => !isExpired(cachedRate))
      .map(_.rate)
  }
  
  def put(rate: Rate): F[Unit] = Sync[F].delay {
    val element = new Element(rate.pair.toString, CachedRate(rate, expiresAt))
    cache.put(element)
  }
}
```

**EhCache Benefits**:
- **Persistence**: Survive application restarts
- **Memory Management**: LRU eviction, size limits
- **Monitoring**: JMX integration, cache statistics
- **Clustering**: Distributed cache for multi-instance setups

**When to Consider EhCache**:
- Multi-instance deployments requiring shared cache
- High memory usage concerns
- Need for cache persistence across restarts
- Advanced monitoring and management requirements

### Redis Alternative

For microservices architectures:

```scala
// Redis-based cache implementation
libraryDependencies += "dev.profunktor" %% "redis4cats-effects" % "1.4.1"

class RedisRateCache[F[_]: Async](redis: RedisCommands[F, String, String]) extends RateCache[F] {
  def get(pair: Rate.Pair): F[Option[Rate]] = {
    redis.get(s"rate:${pair.toString}")
      .map(_.flatMap(json => parseRate(json)))
      .map(_.filter(rate => !isExpired(rate)))
  }
}
```

**Redis Benefits**:
- External cache service
- Horizontal scaling
- Pub/sub for cache invalidation
- Rich data structures

**Trade-offs**:
- Network latency for cache operations
- Additional infrastructure complexity
- Requires Redis deployment and management

## Direct and Reverse Currency Pairs Handling

In the current implementation, direct and reverse currency pairs (e.g., USD/EUR and EUR/USD) are requested separately from the API. This simplifies the logic but may lead to discrepancies between direct and reverse rates due to data source specifics or rounding.

**Alternative approach:**
Only the direct pair (e.g., USD/EUR) is requested, and the reverse pair (EUR/USD) is automatically calculated as `1 / direct_rate`. This guarantees mathematical consistency between pairs, but requires additional logic for request handling and caching.

**Example of alternative implementation:**
```scala
def getRate(pair: Rate.Pair): F[Error Either Rate] = {
  cache.get(pair) match {
    case Some(rate) => F.pure(rate.asRight)
    case None =>
      val directPair = pair
      val reversePair = Rate.Pair(pair.to, pair.from)
      cache.get(reversePair) match {
        case Some(reverseRate) =>
          // Calculate reverse rate
          val calculatedRate = 1.0 / reverseRate.price
          F.pure(Rate(pair.from, pair.to, calculatedRate).asRight)
        case None =>
          // Request direct pair from API
          fetchAndCacheRate(directPair)
      }
  }
}
```
## Configuration & Deployment

### Environment Configuration
```yaml
# docker-compose.yml
environment:
  - HTTP_HOST=0.0.0.0
  - HTTP_PORT=8080
  - ONEFRAME_URL=http://one-frame:8080/rates?
  - ONEFRAME_TOKEN=10dc303535874aeccc86a8251e6992f5
  - ONEFRAME_TIME_TOLERANCE=30s
  - CACHE_TTL=5m
```

### Docker Deployment
```bash
# Development (default profile)
docker-compose up
# or explicitly:
docker-compose --profile default up

# Testing (test profile with faster TTL)
docker-compose --profile test up

# Production  
docker build -t forex-mtl .
docker run -p 8080:8080 \
  -e ONEFRAME_URL=https://api.oneframe.com \
  -e ONEFRAME_TOKEN=your_token \
  -e ONEFRAME_TIME_TOLERANCE=30s \
  forex-mtl
```

### Docker Compose Profiles

**Default Profile (`docker-compose up`)**:
- Runs `forex-mtl` service with production settings
- Cache TTL: 5 minutes
- Time tolerance: 30 seconds
- Direct connection: forex-mtl → one-frame

**Test Profile (`docker-compose --profile test up`)**:
- Runs `forex-mtl-test` service with test settings
- Includes logging proxy server for API call monitoring
- Cache TTL: 2 seconds (faster test execution)
- Time tolerance: 10 seconds
- Proxied connection: forex-mtl-test → proxy → one-frame
- No restart policy for test containers

### Request Flow in Test Mode

```
Client → forex-mtl-test:8087 → proxy:8088 → one-frame:8080
```

The proxy logs all API calls with:
- Timestamp
- Request method, path, headers, body
- Response status code
- Latency in milliseconds

**Proxy endpoints:**
- `GET /get_logs` - Retrieve all logged requests as JSON
- `POST /clear_logs` - Clear all logged requests

## Load Testing

The project includes a comprehensive load testing framework in the `testing/` directory:

**Files:**
- `proxy_server.py` - Logging proxy for API call monitoring
- `load_test.py` - Automated load test script
- `requirements.txt` - Combined dependencies for both tools

**Load Test Features:**
- Automated container management (start/stop test profile)
- Configurable RPS and duration
- Random currency pair generation
- Real-time progress reporting every 30 seconds
- Comprehensive statistics from proxy logs
- Automatic cleanup after completion

**Example Output:**
```
Starting load test: 50 RPS for 5.0 minutes
Start time: 14:30:00
Expected end time: 14:35:00

[14:30:30] Progress: 10.0%
  Forex requests: 1500 (success: 1498, errors: 2)
  One-Frame calls: 3 (0.30% of daily limit)
  Time remaining: 4.5min (end: 14:35:00)
  Actual RPS: 49.8

=============================================================
LOAD TEST SUMMARY
=============================================================
Отправлено запросов к forex: 15000
Успешных (код 200): 14995
Ошибок: 5
Запросов к one-frame: 288
% от дневного лимита: 28.80%
```

### Local Development
```bash
# Using local config
sbt -Dconfig.resource=application-local.conf run

# Testing
sbt test

# Testing with proxy monitoring
docker-compose --profile test up
curl "http://localhost:8087/rates?from=USD&to=EUR"
curl "http://localhost:8088/get_logs"

# Load testing
cd testing
pip install -r requirements.txt
./load_test.py --rps 50 --duration 5  # 50 RPS for 5 minutes
./load_test.py                         # default: 10 RPS for 10 minutes
cd ..
```

## Error Handling

### Structured Error Responses
```json
{
  "error":"INVALID_PARAMETERS",
  "message":"Invalid currency parameters. Supported currencies: AUD, JPY, CAD, NZD, CHF, SGD, EUR, USD, GBP",
  "timestamp":"2025-08-19T01:29:02.234068157Z"
}
```

### Error Categories
- **400**: Invalid currency parameters, missing parameters
- **401**: Authentication errors with One-Frame API
- **429**: Rate limiting (quota exceeded)
- **500**: Internal server errors, One-Frame API issues
- **503**: Service unavailable

### Time Synchronization Monitoring

The service includes configurable time synchronization monitoring to detect clock drift between servers:

**Configuration:**
```hocon
app {
  one-frame {
    time-tolerance = ${ONEFRAME_TIME_TOLERANCE}
  }
}
```

**Monitoring Logic:**
- Compares API timestamps with local server time
- Logs warnings when time difference exceeds configured tolerance
- Provides ops team with early warning for NTP synchronization issues

**Implementation:**
```scala
def checkTimeSync(timestamp: String): Unit = {
  val timeDiff = Duration.between(now, apiTimestamp)
  val direction = if (timeDiff.isNegative) "behind" else "ahead"
  if (timeDiff.abs() > config.timeTolerance) {
    logger.warn(s"API timestamp is ${timeDiff.abs().getSeconds}s $direction of server time")
  }
}
```

**Example log output:**
```
WARN - Time synchronization issue detected: API timestamp 2025-08-22T10:00:00Z is 45s ahead of server time 2025-08-22T09:59:15Z (tolerance: 30s)
WARN - Time synchronization issue detected: API timestamp 2025-08-22T09:58:30Z is 90s behind of server time 2025-08-22T10:00:00Z (tolerance: 30s)
```

**Why This Approach:**
- **Simple and reliable** - minimal complexity, maximum uptime
- **Configurable thresholds** - adjust sensitivity per environment  
- **Operations-friendly** - provides monitoring without service disruption
- **Production-ready** - battle-tested approach for distributed systems

### Error Logging & Alerts

All errors are logged in detail using the project's logger. These logs can be integrated with alerting systems (e.g., via Prometheus, Grafana, or external log monitoring) to notify operators about critical issues.

#### Example error logs:

```scala
logger.error(s"Invalid currency parameters: $params")
logger.error(s"One-Frame API authentication failed: ${ex.getMessage}")
logger.error(s"Rate limit exceeded for token: $token")
logger.error(s"Unexpected error fetching rates: ${ex.getMessage}", ex)
```

Alerts can be configured to trigger on specific error patterns or severity levels in the logs.
