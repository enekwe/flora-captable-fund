/**
 * Carta OAuth Authentication Service
 * Handles OAuth flow, token management, and credential security
 */

const axios = require('axios');
const jwt = require('jsonwebtoken');
const crypto = require('crypto');

class CartaAuthService {
  constructor() {
    this.clientId = process.env.CARTA_CLIENT_ID;
    this.clientSecret = process.env.CARTA_CLIENT_SECRET;
    this.authUrl = process.env.CARTA_AUTH_URL;
    this.redirectUri = process.env.CARTA_REDIRECT_URI;
    this.webhookSecret = process.env.CARTA_WEBHOOK_SECRET;
    this.environment = process.env.CARTA_ENV || 'sandbox';
  }

  /**
   * Generate OAuth authorization URL
   * @param {string} state - CSRF protection state
   * @param {string} organizationId - Flora organization ID
   * @returns {string} Authorization URL
   */
  getAuthorizationUrl(state, organizationId) {
    if (!this.clientId) {
      throw new Error('CARTA_CLIENT_ID not configured');
    }

    const stateToken = state || crypto.randomBytes(16).toString('hex');

    // Store state in cache for validation
    const cacheKey = `carta_oauth_state_${stateToken}`;
    // Note: Implement Redis cache storage here

    const params = new URLSearchParams({
      client_id: this.clientId,
      redirect_uri: this.redirectUri,
      response_type: 'code',
      scope: 'read write webhooks offline_access',
      state: stateToken,
      organization_id: organizationId
    });

    return `${this.authUrl}/authorize?${params.toString()}`;
  }

  /**
   * Exchange authorization code for access token
   * @param {string} code - Authorization code from Carta
   * @returns {Object} Token response
   */
  async exchangeCodeForToken(code) {
    try {
      const response = await axios.post(
        `${this.authUrl}/token`,
        {
          grant_type: 'authorization_code',
          client_id: this.clientId,
          client_secret: this.clientSecret,
          code: code,
          redirect_uri: this.redirectUri
        },
        {
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded'
          }
        }
      );

      const tokenData = {
        accessToken: response.data.access_token,
        refreshToken: response.data.refresh_token,
        expiresIn: response.data.expires_in,
        tokenType: response.data.token_type,
        scope: response.data.scope,
        expiresAt: new Date(Date.now() + (response.data.expires_in * 1000))
      };

      return tokenData;
    } catch (error) {
      console.error('Token exchange failed:', error.response?.data);
      throw new Error(`Carta token exchange failed: ${error.response?.data?.error_description || error.message}`);
    }
  }

  /**
   * Refresh an expired access token
   * @param {string} refreshToken - Refresh token
   * @returns {Object} New token data
   */
  async refreshAccessToken(refreshToken) {
    try {
      const response = await axios.post(
        `${this.authUrl}/token`,
        {
          grant_type: 'refresh_token',
          client_id: this.clientId,
          client_secret: this.clientSecret,
          refresh_token: refreshToken
        },
        {
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded'
          }
        }
      );

      return {
        accessToken: response.data.access_token,
        refreshToken: response.data.refresh_token || refreshToken,
        expiresIn: response.data.expires_in,
        expiresAt: new Date(Date.now() + (response.data.expires_in * 1000))
      };
    } catch (error) {
      console.error('Token refresh failed:', error.response?.data);

      // If refresh fails, need to re-authenticate
      if (error.response?.status === 401) {
        throw new Error('Refresh token expired - re-authentication required');
      }

      throw error;
    }
  }

  /**
   * Revoke access token (for disconnect)
   * @param {string} token - Access or refresh token to revoke
   */
  async revokeToken(token) {
    try {
      await axios.post(
        `${this.authUrl}/revoke`,
        {
          token: token,
          client_id: this.clientId,
          client_secret: this.clientSecret
        },
        {
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded'
          }
        }
      );

      return { success: true };
    } catch (error) {
      console.error('Token revocation failed:', error.response?.data);
      throw error;
    }
  }

  /**
   * Validate webhook signature
   * @param {string} payload - Webhook payload
   * @param {string} signature - Carta signature header
   * @returns {boolean} Valid signature
   */
  validateWebhookSignature(payload, signature) {
    if (!this.webhookSecret) {
      console.warn('CARTA_WEBHOOK_SECRET not configured');
      return false;
    }

    const expectedSignature = crypto
      .createHmac('sha256', this.webhookSecret)
      .update(payload)
      .digest('hex');

    return crypto.timingSafeEqual(
      Buffer.from(signature),
      Buffer.from(expectedSignature)
    );
  }

  /**
   * Get client credentials token (for server-to-server)
   * @returns {Object} Token data
   */
  async getClientCredentialsToken() {
    try {
      const response = await axios.post(
        `${this.authUrl}/token`,
        {
          grant_type: 'client_credentials',
          client_id: this.clientId,
          client_secret: this.clientSecret,
          scope: 'read write'
        },
        {
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded'
          }
        }
      );

      return {
        accessToken: response.data.access_token,
        expiresIn: response.data.expires_in,
        tokenType: response.data.token_type,
        expiresAt: new Date(Date.now() + (response.data.expires_in * 1000))
      };
    } catch (error) {
      console.error('Client credentials grant failed:', error.response?.data);
      throw error;
    }
  }

  /**
   * Encrypt sensitive data for storage
   * @param {string} data - Data to encrypt
   * @returns {string} Encrypted data
   */
  encryptData(data) {
    const algorithm = 'aes-256-cbc';
    const key = Buffer.from(process.env.ENCRYPTION_KEY || 'default-key-change-in-production', 'utf8').slice(0, 32);
    const iv = crypto.randomBytes(16);

    const cipher = crypto.createCipheriv(algorithm, key, iv);
    let encrypted = cipher.update(data, 'utf8', 'hex');
    encrypted += cipher.final('hex');

    return iv.toString('hex') + ':' + encrypted;
  }

  /**
   * Decrypt sensitive data from storage
   * @param {string} encryptedData - Encrypted data
   * @returns {string} Decrypted data
   */
  decryptData(encryptedData) {
    const algorithm = 'aes-256-cbc';
    const key = Buffer.from(process.env.ENCRYPTION_KEY || 'default-key-change-in-production', 'utf8').slice(0, 32);

    const parts = encryptedData.split(':');
    const iv = Buffer.from(parts[0], 'hex');
    const encrypted = parts[1];

    const decipher = crypto.createDecipheriv(algorithm, key, iv);
    let decrypted = decipher.update(encrypted, 'hex', 'utf8');
    decrypted += decipher.final('utf8');

    return decrypted;
  }

  /**
   * Test the current configuration
   * @returns {Object} Configuration status
   */
  getConfigurationStatus() {
    return {
      environment: this.environment,
      clientIdConfigured: !!this.clientId,
      clientSecretConfigured: !!this.clientSecret,
      authUrlConfigured: !!this.authUrl,
      redirectUriConfigured: !!this.redirectUri,
      webhookSecretConfigured: !!this.webhookSecret,
      authUrl: this.authUrl,
      redirectUri: this.redirectUri
    };
  }
}

module.exports = new CartaAuthService();