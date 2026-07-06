/**
 * Carta API Client
 * Handles all API requests to Carta with rate limiting and retry logic
 */

const axios = require('axios');
const cartaAuthService = require('./cartaAuthService');

class CartaApiClient {
  constructor() {
    this.baseUrl = process.env.CARTA_API_BASE_URL || 'https://sandbox-api.carta.com/v2';
    this.rateLimit = parseInt(process.env.CARTA_RATE_LIMIT) || 100;
    this.rateWindow = parseInt(process.env.CARTA_RATE_WINDOW) || 60000; // 1 minute
    this.requestQueue = [];
    this.requestCount = 0;
    this.windowStart = Date.now();
  }

  /**
   * Make authenticated API request
   * @param {Object} options - Request options
   * @returns {Object} API response
   */
  async request(options) {
    // Rate limiting check
    await this.checkRateLimit();

    // Get or refresh token
    const token = await this.getValidToken(options.organizationId);

    try {
      const response = await axios({
        ...options,
        baseURL: this.baseUrl,
        headers: {
          ...options.headers,
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json',
          'X-Carta-API-Version': '2.0'
        },
        timeout: options.timeout || 30000
      });

      return response.data;
    } catch (error) {
      // Handle rate limiting
      if (error.response?.status === 429) {
        const retryAfter = error.response.headers['retry-after'] || 60;
        console.log(`Rate limited. Retrying after ${retryAfter} seconds`);
        await this.sleep(retryAfter * 1000);
        return this.request(options);
      }

      // Handle token expiration
      if (error.response?.status === 401) {
        await this.refreshToken(options.organizationId);
        return this.request(options);
      }

      // Log and throw other errors
      console.error('Carta API request failed:', {
        url: options.url,
        status: error.response?.status,
        data: error.response?.data
      });

      throw error;
    }
  }

  /**
   * Rate limiting implementation
   */
  async checkRateLimit() {
    const now = Date.now();

    // Reset window if needed
    if (now - this.windowStart > this.rateWindow) {
      this.requestCount = 0;
      this.windowStart = now;
    }

    // If at limit, wait until window resets
    if (this.requestCount >= this.rateLimit) {
      const waitTime = this.rateWindow - (now - this.windowStart);
      console.log(`Rate limit reached. Waiting ${waitTime}ms`);
      await this.sleep(waitTime);
      this.requestCount = 0;
      this.windowStart = Date.now();
    }

    this.requestCount++;
  }

  /**
   * Get valid access token (from cache or refresh)
   * @param {string} organizationId - Organization ID
   * @returns {string} Valid access token
   */
  async getValidToken(organizationId) {
    // This should be implemented with proper token storage
    // For now, returning placeholder
    const CartaConnection = require('../../models/carta/CartaConnection');

    const connection = await CartaConnection.findOne({
      organizationId,
      status: 'active'
    });

    if (!connection) {
      throw new Error('No active Carta connection found');
    }

    // Check if token is expired
    if (connection.tokenExpiresAt < new Date()) {
      const newToken = await cartaAuthService.refreshAccessToken(connection.refreshToken);

      // Update stored token
      connection.accessToken = cartaAuthService.encryptData(newToken.accessToken);
      connection.refreshToken = cartaAuthService.encryptData(newToken.refreshToken);
      connection.tokenExpiresAt = newToken.expiresAt;
      await connection.save();

      return newToken.accessToken;
    }

    return cartaAuthService.decryptData(connection.accessToken);
  }

  /**
   * Sleep helper
   * @param {number} ms - Milliseconds to sleep
   */
  sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  /**
   * Test API connection
   * @returns {Object} Test result
   */
  async testConnection() {
    try {
      // Try to get a client credentials token
      const token = await cartaAuthService.getClientCredentialsToken();

      // Make a simple API call
      const response = await axios.get(`${this.baseUrl}/ping`, {
        headers: {
          'Authorization': `Bearer ${token.accessToken}`,
          'X-Carta-API-Version': '2.0'
        }
      });

      return {
        connected: true,
        environment: process.env.CARTA_ENV,
        apiVersion: response.data.version || '2.0',
        timestamp: new Date().toISOString()
      };
    } catch (error) {
      return {
        connected: false,
        error: error.message,
        environment: process.env.CARTA_ENV,
        timestamp: new Date().toISOString()
      };
    }
  }

  // ============ Cap Table Methods ============

  /**
   * Get organization details
   */
  async getOrganization(organizationId) {
    return this.request({
      method: 'GET',
      url: `/organizations/${organizationId}`,
      organizationId
    });
  }

  /**
   * Get cap table
   */
  async getCapTable(organizationId, companyId) {
    return this.request({
      method: 'GET',
      url: `/companies/${companyId}/cap-table`,
      organizationId
    });
  }

  /**
   * Get stakeholders
   */
  async getStakeholders(organizationId, companyId, params = {}) {
    return this.request({
      method: 'GET',
      url: `/companies/${companyId}/stakeholders`,
      params,
      organizationId
    });
  }

  /**
   * Get securities
   */
  async getSecurities(organizationId, companyId, params = {}) {
    return this.request({
      method: 'GET',
      url: `/companies/${companyId}/securities`,
      params,
      organizationId
    });
  }

  // ============ Fund Methods ============

  /**
   * Get funds
   */
  async getFunds(organizationId, params = {}) {
    return this.request({
      method: 'GET',
      url: '/funds',
      params,
      organizationId
    });
  }

  /**
   * Get fund details
   */
  async getFund(organizationId, fundId) {
    return this.request({
      method: 'GET',
      url: `/funds/${fundId}`,
      organizationId
    });
  }

  /**
   * Get fund investors
   */
  async getFundInvestors(organizationId, fundId, params = {}) {
    return this.request({
      method: 'GET',
      url: `/funds/${fundId}/investors`,
      params,
      organizationId
    });
  }

  /**
   * Get fund investments
   */
  async getFundInvestments(organizationId, fundId, params = {}) {
    return this.request({
      method: 'GET',
      url: `/funds/${fundId}/investments`,
      params,
      organizationId
    });
  }

  // ============ Capital Call Methods ============

  /**
   * Get capital calls
   */
  async getCapitalCalls(organizationId, fundId, params = {}) {
    return this.request({
      method: 'GET',
      url: `/funds/${fundId}/capital-calls`,
      params,
      organizationId
    });
  }

  /**
   * Create capital call
   */
  async createCapitalCall(organizationId, fundId, data) {
    return this.request({
      method: 'POST',
      url: `/funds/${fundId}/capital-calls`,
      data,
      organizationId
    });
  }

  /**
   * Update capital call
   */
  async updateCapitalCall(organizationId, fundId, callId, data) {
    return this.request({
      method: 'PUT',
      url: `/funds/${fundId}/capital-calls/${callId}`,
      data,
      organizationId
    });
  }

  // ============ Distribution Methods ============

  /**
   * Get distributions
   */
  async getDistributions(organizationId, fundId, params = {}) {
    return this.request({
      method: 'GET',
      url: `/funds/${fundId}/distributions`,
      params,
      organizationId
    });
  }

  /**
   * Create distribution
   */
  async createDistribution(organizationId, fundId, data) {
    return this.request({
      method: 'POST',
      url: `/funds/${fundId}/distributions`,
      data,
      organizationId
    });
  }

  // ============ Document Methods ============

  /**
   * Get documents
   */
  async getDocuments(organizationId, entityType, entityId, params = {}) {
    return this.request({
      method: 'GET',
      url: `/${entityType}/${entityId}/documents`,
      params,
      organizationId
    });
  }

  /**
   * Upload document
   */
  async uploadDocument(organizationId, entityType, entityId, formData) {
    return this.request({
      method: 'POST',
      url: `/${entityType}/${entityId}/documents`,
      data: formData,
      headers: {
        'Content-Type': 'multipart/form-data'
      },
      organizationId
    });
  }

  // ============ Webhook Methods ============

  /**
   * Register webhook
   */
  async registerWebhook(organizationId, config) {
    return this.request({
      method: 'POST',
      url: '/webhooks',
      data: config,
      organizationId
    });
  }

  /**
   * List webhooks
   */
  async listWebhooks(organizationId) {
    return this.request({
      method: 'GET',
      url: '/webhooks',
      organizationId
    });
  }

  /**
   * Delete webhook
   */
  async deleteWebhook(organizationId, webhookId) {
    return this.request({
      method: 'DELETE',
      url: `/webhooks/${webhookId}`,
      organizationId
    });
  }
}

module.exports = new CartaApiClient();