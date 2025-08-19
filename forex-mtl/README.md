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
2. **RateCache** - TTL-based concurrent cache using TrieMap
3. **CachedOneFrame** - Main service orchestrating cache and API calls

## Meeting One-Frame API Limitations

### The Challenge
- **Target Load**: 10,000 requests/day
- **API Limit**: 1,000 requests/day
- **Required Efficiency**: 10:1 cache hit ratio

### Solution Strategy

#### 1. TTL-Based Caching (5 minutes)
```scala
val expiresAt = apiTimestamp.plusMillis(ttl.toMillis)
cache.put(rate.pair, CachedRate(rate, expiresAt))
```

#### 2. Batch API Optimization
```scala
// Instead of: 1 request per currency pair
// We do: 1 request for multiple pairs
val pairsToFetch = (allCachedPairs :+ requestedPair).distinct
client.getBatch(pairsToFetch)
```

#### 3. Smart Cache Management
- **Simplified Strategy**: On cache miss, refresh ALL cached pairs + requested pair
- **TrieMap Storage**: Thread-safe concurrent map for high-performance access
- **TTL-Based Expiration**: Automatic cleanup of expired rates

### Efficiency Analysis

**Best Case Scenario**: 
- 288 API calls/day (one every 5 minutes for all active pairs)
- Additional calls only when new currency pairs are requested

**Result**: Comfortably within 1000 API calls/day limit 

## Thread Safety Implementation

### Current Solution: Synchronized Method

```scala
private def getCurrencyRate(pair: Rate.Pair): F[Error Either Rate] = {
  this.synchronized {
    cache.get(pair).flatMap {
      // Entire cache check + API call logic
    }
  }
}
```

**Why This Works**:
- **Atomic Operations**: Entire cache-check-and-update cycle is synchronized
- **No Race Conditions**: Only one thread can execute getCurrencyRate() at a time
- **Performance Adequate**: At ~0.12 RPS (10k requests/day), blocking is negligible
- **Simple & Reliable**: Easy to understand and maintain

### Alternative Thread Safety Approaches

#### 1. Double-Checked Locking Pattern
```scala
private def getCurrencyRate(pair: Rate.Pair): F[Error Either Rate] = {
  cache.get(pair) match {
    case Some(rate) => F.pure(rate.asRight)
    case None => 
      this.synchronized {
        // Double-check: another thread might have updated cache
        cache.get(pair) match {
          case Some(rate) => F.pure(rate.asRight)
          case None => performAPICall(pair)
        }
      }
  }
}
```
- **Pros**: Better read performance, synchronized only on cache miss
- **Cons**: More complex, error-prone implementation

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

### Why Synchronized Was Chosen

1. **Performance Requirements**: At 0.12-0.35 RPS, method-level synchronization has negligible impact
2. **Simplicity**: Single point of synchronization, easy to reason about
3. **Reliability**: No complex lock management or potential deadlocks
4. **Maintainability**: Future developers can easily understand and modify

## Cache Implementation Alternatives

### Current Solution: In-Memory TrieMap
```scala
private val cache = TrieMap[Rate.Pair, CachedRate]()
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

## Configuration & Deployment

### Environment Configuration
```yaml
# docker-compose.yml
environment:
  - HTTP_HOST=0.0.0.0
  - HTTP_PORT=8080
  - ONEFRAME_URL=http://one-frame:8080/rates?
  - ONEFRAME_TOKEN=10dc303535874aeccc86a8251e6992f5
  - CACHE_TTL=5m
```

### Docker Deployment
```bash
# Development
docker-compose up

# Production  
docker build -t forex-mtl .
docker run -p 8080:8080 \
  -e ONEFRAME_URL=https://api.oneframe.com \
  -e ONEFRAME_TOKEN=your_token \
  forex-mtl
```

### Local Development
```bash
# Using local config
sbt -Dconfig.resource=application-local.conf run

# Testing
sbt test
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
