/**
 * Carta Rate Limiter Service
 * Implements token bucket algorithm for API rate limiting
 * Provides per-endpoint limits, automatic retry with backoff, and monitoring
 */

const logger = require('../../utils/logger');

class CartaRateLimiter {
  constructor() {
    // Global rate limits (can be overridden per endpoint)
    this.config = {
      globalLimit: parseInt(process.env.CARTA_RATE_LIMIT) || 100,
      globalWindow: parseInt(process.env.CARTA_RATE_WINDOW) || 60000, // 1 minute in ms
      burstLimit: parseInt(process.env.CARTA_BURST_LIMIT) || 20,
      enableAdaptiveRateLimit: process.env.CARTA_ADAPTIVE_RATE_LIMIT === 'true'
    };

    // Token buckets for different endpoints
    this.buckets = new Map();

    // Per-endpoint configurations
    this.endpointConfigs = {
      // High-volume endpoints
      '/stakeholders': { limit: 150, window: 60000, burst: 30 },
      '/securities': { limit: 150, window: 60000, burst: 30 },
      '/cap-table': { limit: 100, window: 60000, burst: 20 },

      // Medium-volume endpoints
      '/funds': { limit: 100, window: 60000, burst: 20 },
      '/investments': { limit: 100, window: 60000, burst: 20 },
      '/valuations': { limit: 100, window: 60000, burst: 20 },

      // Low-volume endpoints (mutations)
      '/capital-calls': { limit: 50, window: 60000, burst: 10 },
      '/distributions': { limit: 50, window: 60000, burst: 10 },
      '/documents': { limit: 30, window: 60000, burst: 5 },

      // Webhook endpoints
      '/webhooks': { limit: 20, window: 60000, burst: 5 }
    };

    // Statistics tracking
    this.stats = {
      totalRequests: 0,
      rateLimitedRequests: 0,
      retriedRequests: 0,
      failedRequests: 0,
      averageWaitTime: 0,
      endpointStats: new Map()
    };

    // Adaptive rate limiting state
    this.adaptiveState = {
      consecutiveErrors: 0,
      lastErrorTime: null,
      currentMultiplier: 1.0,
      minMultiplier: 0.5,
      maxMultiplier: 1.0
    };

    // Cleanup old buckets periodically
    this.startCleanupInterval();
  }

  /**
   * Check if request can proceed under rate limits
   * @param {String} endpoint - API endpoint path
   * @param {String} connectionId - Connection identifier
   * @returns {Promise<Object>} Rate limit check result
   */
  async checkRateLimit(endpoint, connectionId = 'default') {
    const bucketKey = `${connectionId}:${endpoint}`;
    const config = this.getEndpointConfig(endpoint);

    // Get or create bucket
    let bucket = this.buckets.get(bucketKey);
    if (!bucket) {
      bucket = this.createBucket(config);
      this.buckets.set(bucketKey, bucket);
    }

    // Update token count
    this.refillTokens(bucket, config);

    // Check if tokens available
    if (bucket.tokens >= 1) {
      bucket.tokens -= 1;
      bucket.lastRequest = Date.now();

      this.updateStats(endpoint, 'allowed');

      return {
        allowed: true,
        tokens: bucket.tokens,
        retryAfter: 0,
        endpoint
      };
    }

    // Rate limited - calculate retry time
    const retryAfter = this.calculateRetryTime(bucket, config);

    this.updateStats(endpoint, 'limited');

    logger.warn('Rate limit reached', {
      endpoint,
      connectionId,
      retryAfter,
      currentTokens: bucket.tokens
    });

    return {
      allowed: false,
      tokens: bucket.tokens,
      retryAfter,
      endpoint
    };
  }

  /**
   * Wait for rate limit to allow request
   * @param {String} endpoint - API endpoint path
   * @param {String} connectionId - Connection identifier
   * @param {Number} maxWaitTime - Maximum time to wait in ms
   * @returns {Promise<Boolean>} Whether request can proceed
   */
  async waitForRateLimit(endpoint, connectionId = 'default', maxWaitTime = 60000) {
    const startTime = Date.now();

    while (Date.now() - startTime < maxWaitTime) {
      const check = await this.checkRateLimit(endpoint, connectionId);

      if (check.allowed) {
        return true;
      }

      if (check.retryAfter > maxWaitTime - (Date.now() - startTime)) {
        logger.warn('Max wait time exceeded', {
          endpoint,
          connectionId,
          waited: Date.now() - startTime
        });
        return false;
      }

      await this.sleep(Math.min(check.retryAfter, 1000));
    }

    return false;
  }

  /**
   * Execute request with automatic retry on rate limit
   * @param {Function} requestFn - Function that executes the request
   * @param {String} endpoint - API endpoint path
   * @param {String} connectionId - Connection identifier
   * @param {Object} options - Retry options
   * @returns {Promise<*>} Request result
   */
  async executeWithRetry(requestFn, endpoint, connectionId = 'default', options = {}) {
    const {
      maxRetries = 3,
      maxWaitTime = 300000, // 5 minutes
      backoffMultiplier = 2,
      initialDelay = 1000
    } = options;

    let attempt = 0;
    let delay = initialDelay;

    while (attempt <= maxRetries) {
      try {
        // Check rate limit
        const canProceed = await this.waitForRateLimit(endpoint, connectionId, maxWaitTime);

        if (!canProceed) {
          throw new Error('Rate limit wait time exceeded');
        }

        // Execute request
        const result = await requestFn();

        // Success - reset adaptive state if needed
        if (this.adaptiveState.consecutiveErrors > 0) {
          this.resetAdaptiveState();
        }

        return result;

      } catch (error) {
        attempt++;

        // Check if error is rate limit related
        if (this.isRateLimitError(error)) {
          this.handleRateLimitError(error, endpoint, connectionId);

          if (attempt <= maxRetries) {
            logger.info('Retrying after rate limit', {
              endpoint,
              attempt,
              maxRetries,
              delay
            });

            await this.sleep(delay);
            delay *= backoffMultiplier;

            this.updateStats(endpoint, 'retried');
            continue;
          }
        }

        // Non-rate-limit error or max retries reached
        this.updateStats(endpoint, 'failed');
        throw error;
      }
    }

    throw new Error(`Max retries (${maxRetries}) exceeded for ${endpoint}`);
  }

  /**
   * Create new token bucket
   * @param {Object} config - Bucket configuration
   * @returns {Object} Token bucket
   */
  createBucket(config) {
    return {
      tokens: config.limit,
      maxTokens: config.limit,
      refillRate: config.limit / (config.window / 1000), // tokens per second
      lastRefill: Date.now(),
      lastRequest: null,
      burstTokens: config.burst
    };
  }

  /**
   * Refill tokens in bucket based on time elapsed
   * @param {Object} bucket - Token bucket
   * @param {Object} config - Bucket configuration
   */
  refillTokens(bucket, config) {
    const now = Date.now();
    const timeSinceRefill = now - bucket.lastRefill;
    const secondsElapsed = timeSinceRefill / 1000;

    // Calculate tokens to add
    const tokensToAdd = secondsElapsed * bucket.refillRate;

    // Apply adaptive rate limiting if enabled
    let effectiveTokens = tokensToAdd;
    if (this.config.enableAdaptiveRateLimit) {
      effectiveTokens = tokensToAdd * this.adaptiveState.currentMultiplier;
    }

    // Refill tokens up to max
    bucket.tokens = Math.min(bucket.maxTokens, bucket.tokens + effectiveTokens);
    bucket.lastRefill = now;
  }

  /**
   * Calculate retry time in milliseconds
   * @param {Object} bucket - Token bucket
   * @param {Object} config - Bucket configuration
   * @returns {Number} Retry time in ms
   */
  calculateRetryTime(bucket, config) {
    const tokensNeeded = 1 - bucket.tokens;
    const timeNeeded = (tokensNeeded / bucket.refillRate) * 1000;

    // Add jitter to prevent thundering herd
    const jitter = Math.random() * 100;

    return Math.ceil(timeNeeded + jitter);
  }

  /**
   * Get configuration for endpoint
   * @param {String} endpoint - API endpoint path
   * @returns {Object} Endpoint configuration
   */
  getEndpointConfig(endpoint) {
    // Try to match endpoint pattern
    for (const [pattern, config] of Object.entries(this.endpointConfigs)) {
      if (endpoint.includes(pattern)) {
        return config;
      }
    }

    // Return global config as default
    return {
      limit: this.config.globalLimit,
      window: this.config.globalWindow,
      burst: this.config.burstLimit
    };
  }

  /**
   * Handle rate limit error from API
   * @param {Error} error - Error object
   * @param {String} endpoint - API endpoint
   * @param {String} connectionId - Connection identifier
   */
  handleRateLimitError(error, endpoint, connectionId) {
    this.adaptiveState.consecutiveErrors++;
    this.adaptiveState.lastErrorTime = Date.now();

    // Reduce rate if adaptive limiting is enabled
    if (this.config.enableAdaptiveRateLimit) {
      const newMultiplier = Math.max(
        this.adaptiveState.minMultiplier,
        this.adaptiveState.currentMultiplier * 0.75
      );

      this.adaptiveState.currentMultiplier = newMultiplier;

      logger.info('Adaptive rate limit reduced', {
        endpoint,
        connectionId,
        newMultiplier,
        consecutiveErrors: this.adaptiveState.consecutiveErrors
      });
    }

    // Extract retry-after header if available
    const retryAfter = error.response?.headers?.['retry-after'];
    if (retryAfter) {
      logger.info('Carta API retry-after header', {
        endpoint,
        retryAfter
      });
    }
  }

  /**
   * Reset adaptive state after successful requests
   */
  resetAdaptiveState() {
    const previousMultiplier = this.adaptiveState.currentMultiplier;

    this.adaptiveState.consecutiveErrors = 0;
    this.adaptiveState.currentMultiplier = Math.min(
      this.adaptiveState.maxMultiplier,
      this.adaptiveState.currentMultiplier * 1.1
    );

    if (previousMultiplier !== this.adaptiveState.currentMultiplier) {
      logger.info('Adaptive rate limit increased', {
        newMultiplier: this.adaptiveState.currentMultiplier
      });
    }
  }

  /**
   * Check if error is rate limit related
   * @param {Error} error - Error object
   * @returns {Boolean} Is rate limit error
   */
  isRateLimitError(error) {
    if (!error.response) return false;

    const status = error.response.status;
    const rateLimitStatuses = [429, 503];

    return rateLimitStatuses.includes(status);
  }

  /**
   * Update statistics
   * @param {String} endpoint - API endpoint
   * @param {String} type - Event type (allowed, limited, retried, failed)
   */
  updateStats(endpoint, type) {
    this.stats.totalRequests++;

    switch (type) {
      case 'limited':
        this.stats.rateLimitedRequests++;
        break;
      case 'retried':
        this.stats.retriedRequests++;
        break;
      case 'failed':
        this.stats.failedRequests++;
        break;
    }

    // Update endpoint-specific stats
    if (!this.stats.endpointStats.has(endpoint)) {
      this.stats.endpointStats.set(endpoint, {
        total: 0,
        limited: 0,
        retried: 0,
        failed: 0
      });
    }

    const endpointStats = this.stats.endpointStats.get(endpoint);
    endpointStats.total++;

    if (type !== 'allowed') {
      endpointStats[type]++;
    }
  }

  /**
   * Get current statistics
   * @returns {Object} Rate limiter statistics
   */
  getStatistics() {
    const endpointStatsObject = {};
    this.stats.endpointStats.forEach((stats, endpoint) => {
      endpointStatsObject[endpoint] = stats;
    });

    return {
      global: {
        totalRequests: this.stats.totalRequests,
        rateLimitedRequests: this.stats.rateLimitedRequests,
        retriedRequests: this.stats.retriedRequests,
        failedRequests: this.stats.failedRequests,
        rateLimitRate: this.stats.totalRequests > 0
          ? (this.stats.rateLimitedRequests / this.stats.totalRequests * 100).toFixed(2) + '%'
          : '0%'
      },
      endpoints: endpointStatsObject,
      adaptive: {
        enabled: this.config.enableAdaptiveRateLimit,
        currentMultiplier: this.adaptiveState.currentMultiplier,
        consecutiveErrors: this.adaptiveState.consecutiveErrors,
        lastErrorTime: this.adaptiveState.lastErrorTime
      },
      buckets: {
        active: this.buckets.size
      }
    };
  }

  /**
   * Reset bucket for connection/endpoint
   * @param {String} endpoint - API endpoint
   * @param {String} connectionId - Connection identifier
   */
  resetBucket(endpoint, connectionId = 'default') {
    const bucketKey = `${connectionId}:${endpoint}`;
    this.buckets.delete(bucketKey);

    logger.info('Rate limit bucket reset', {
      endpoint,
      connectionId
    });
  }

  /**
   * Reset all buckets
   */
  resetAllBuckets() {
    this.buckets.clear();
    logger.info('All rate limit buckets reset');
  }

  /**
   * Get current bucket status
   * @param {String} endpoint - API endpoint
   * @param {String} connectionId - Connection identifier
   * @returns {Object} Bucket status
   */
  getBucketStatus(endpoint, connectionId = 'default') {
    const bucketKey = `${connectionId}:${endpoint}`;
    const bucket = this.buckets.get(bucketKey);

    if (!bucket) {
      return {
        exists: false,
        endpoint,
        connectionId
      };
    }

    const config = this.getEndpointConfig(endpoint);

    return {
      exists: true,
      endpoint,
      connectionId,
      tokens: Math.floor(bucket.tokens),
      maxTokens: bucket.maxTokens,
      utilizationRate: ((bucket.maxTokens - bucket.tokens) / bucket.maxTokens * 100).toFixed(2) + '%',
      lastRefill: new Date(bucket.lastRefill),
      lastRequest: bucket.lastRequest ? new Date(bucket.lastRequest) : null,
      refillRate: bucket.refillRate,
      nextRefill: new Date(bucket.lastRefill + config.window)
    };
  }

  /**
   * Start cleanup interval for old buckets
   */
  startCleanupInterval() {
    // Clean up buckets older than 5 minutes
    const cleanupInterval = 300000; // 5 minutes
    const bucketTimeout = 600000; // 10 minutes

    setInterval(() => {
      const now = Date.now();
      let cleaned = 0;

      for (const [key, bucket] of this.buckets.entries()) {
        if (bucket.lastRequest && (now - bucket.lastRequest > bucketTimeout)) {
          this.buckets.delete(key);
          cleaned++;
        }
      }

      if (cleaned > 0) {
        logger.debug('Cleaned up rate limit buckets', {
          cleaned,
          remaining: this.buckets.size
        });
      }
    }, cleanupInterval);
  }

  /**
   * Sleep helper
   * @param {Number} ms - Milliseconds to sleep
   * @returns {Promise} Promise that resolves after delay
   */
  sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  /**
   * Configure endpoint-specific limits
   * @param {String} endpoint - API endpoint
   * @param {Object} config - Endpoint configuration
   */
  configureEndpoint(endpoint, config) {
    this.endpointConfigs[endpoint] = {
      limit: config.limit || this.config.globalLimit,
      window: config.window || this.config.globalWindow,
      burst: config.burst || this.config.burstLimit
    };

    logger.info('Endpoint rate limit configured', {
      endpoint,
      config: this.endpointConfigs[endpoint]
    });
  }

  /**
   * Enable or disable adaptive rate limiting
   * @param {Boolean} enabled - Enable adaptive rate limiting
   */
  setAdaptiveRateLimit(enabled) {
    this.config.enableAdaptiveRateLimit = enabled;

    if (!enabled) {
      this.resetAdaptiveState();
    }

    logger.info('Adaptive rate limiting ' + (enabled ? 'enabled' : 'disabled'));
  }
}

module.exports = new CartaRateLimiter();
