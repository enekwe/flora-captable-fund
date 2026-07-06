/**
 * Carta Webhook Service
 * Handles incoming webhooks from Carta with event routing and queue-based processing
 */

const Bull = require('bull');
const CartaConnection = require('../../models/carta/CartaConnection');
const CartaSyncLog = require('../../models/carta/CartaSyncLog');
const cartaAuthService = require('./cartaAuthService');
const cartaApiClient = require('./cartaApiClient');
const cartaDataMapper = require('./cartaDataMapper');
const cartaSyncService = require('./cartaSyncService');
const Fund = require('../../models/Fund');
const Investment = require('../../models/Investment');
const Stakeholder = require('../../models/Stakeholder');
const CapTable = require('../../models/CapTable');
const logger = require('../../utils/logger');

class CartaWebhookService {
  constructor() {
    // Initialize Bull queue for async webhook processing
    this.webhookQueue = new Bull('carta-webhooks', {
      redis: {
        host: process.env.REDIS_HOST || 'localhost',
        port: process.env.REDIS_PORT || 6379,
        password: process.env.REDIS_PASSWORD
      },
      defaultJobOptions: {
        attempts: 3,
        backoff: {
          type: 'exponential',
          delay: 5000
        },
        removeOnComplete: 500,
        removeOnFail: 100
      }
    });

    this.setupQueueProcessors();
  }

  /**
   * Setup queue processors for webhook events
   */
  setupQueueProcessors() {
    // Process webhook events
    this.webhookQueue.process('webhook-event', async (job) => {
      return await this.processWebhookEvent(job.data);
    });

    // Queue event handlers
    this.webhookQueue.on('completed', (job, result) => {
      logger.info('Webhook event processed', {
        jobId: job.id,
        eventType: job.data.eventType,
        result
      });
    });

    this.webhookQueue.on('failed', (job, err) => {
      logger.error('Webhook event processing failed', {
        jobId: job.id,
        eventType: job.data.eventType,
        error: err.message,
        stack: err.stack
      });
    });
  }

  /**
   * Process incoming webhook from Carta
   * @param {Object} payload - Webhook payload
   * @param {String} signature - Carta signature header
   * @param {String} cartaOrgId - Carta organization ID
   * @returns {Promise<Object>} Processing result
   */
  async processWebhook(payload, signature, cartaOrgId) {
    const startTime = Date.now();

    try {
      // 1. Validate webhook signature
      const payloadString = typeof payload === 'string' ? payload : JSON.stringify(payload);
      const isValid = cartaAuthService.validateWebhookSignature(payloadString, signature);

      if (!isValid) {
        logger.warn('Invalid webhook signature', { cartaOrgId });
        throw new Error('Invalid webhook signature');
      }

      // 2. Find active connection
      const connection = await CartaConnection.findOne({
        cartaOrganizationId: cartaOrgId,
        status: 'active'
      });

      if (!connection) {
        logger.warn('No active connection found for webhook', { cartaOrgId });
        throw new Error('No active connection found');
      }

      // 3. Parse webhook payload
      const webhookData = typeof payload === 'string' ? JSON.parse(payload) : payload;
      const { event_type, data, event_id, timestamp } = webhookData;

      logger.info('Webhook received', {
        eventType: event_type,
        eventId: event_id,
        cartaOrgId,
        timestamp
      });

      // 4. Queue webhook for processing
      const job = await this.webhookQueue.add('webhook-event', {
        connectionId: connection._id,
        fundId: connection.organizationId,
        eventType: event_type,
        eventId: event_id,
        data: data,
        timestamp: timestamp,
        cartaOrgId
      }, {
        priority: this.getEventPriority(event_type),
        jobId: event_id // Use event ID to prevent duplicate processing
      });

      const processingTime = Date.now() - startTime;

      logger.info('Webhook queued for processing', {
        eventType: event_type,
        jobId: job.id,
        processingTime
      });

      return {
        success: true,
        eventId: event_id,
        jobId: job.id,
        message: 'Webhook queued for processing'
      };

    } catch (error) {
      logger.error('Webhook processing failed', {
        cartaOrgId,
        error: error.message,
        stack: error.stack
      });

      throw error;
    }
  }

  /**
   * Process webhook event (called by queue processor)
   * @param {Object} eventData - Event data
   * @returns {Promise<Object>} Processing result
   */
  async processWebhookEvent(eventData) {
    const { connectionId, fundId, eventType, eventId, data, timestamp } = eventData;

    // Create sync log for webhook event
    const syncLog = new CartaSyncLog({
      connectionId,
      fundId,
      syncType: 'webhook',
      syncDirection: 'carta_to_flora',
      status: 'in_progress',
      trigger: {
        type: 'webhook',
        webhookEvent: eventType,
        webhookEventId: eventId,
        webhookTimestamp: new Date(timestamp)
      }
    });

    await syncLog.save();

    try {
      // Get connection
      const connection = await CartaConnection.findById(connectionId)
        .select('+accessToken +refreshToken');

      if (!connection) {
        throw new Error('Connection not found');
      }

      // Route to appropriate event handler
      let result;

      switch (eventType) {
        case 'cap_table.updated':
        case 'cap_table.snapshot_created':
          result = await this.handleCapTableUpdated(connection, data, eventType, syncLog);
          break;

        case 'stakeholder.created':
          result = await this.handleStakeholderCreated(connection, data, eventType, syncLog);
          break;

        case 'stakeholder.updated':
          result = await this.handleStakeholderUpdated(connection, data, eventType, syncLog);
          break;

        case 'stakeholder.deleted':
          result = await this.handleStakeholderDeleted(connection, data, eventType, syncLog);
          break;

        case 'investment.created':
          result = await this.handleInvestmentCreated(connection, data, eventType, syncLog);
          break;

        case 'investment.updated':
          result = await this.handleInvestmentUpdated(connection, data, eventType, syncLog);
          break;

        case 'valuation.updated':
        case 'valuation.created':
          result = await this.handleValuationUpdated(connection, data, eventType, syncLog);
          break;

        case 'transaction.created':
        case 'transaction.updated':
          result = await this.handleTransactionCreated(connection, data, eventType, syncLog);
          break;

        case 'fund.updated':
          result = await this.handleFundUpdated(connection, data, eventType, syncLog);
          break;

        case 'capital_call.created':
        case 'capital_call.updated':
          result = await this.handleCapitalCallEvent(connection, data, eventType, syncLog);
          break;

        case 'distribution.created':
        case 'distribution.updated':
          result = await this.handleDistributionEvent(connection, data, eventType, syncLog);
          break;

        default:
          logger.warn('Unknown webhook event type', { eventType });
          result = { success: false, message: 'Unknown event type' };
      }

      // Update sync log
      syncLog.summary.totalSynced = result.synced || 1;
      syncLog.summary.totalFailed = result.failed || 0;
      await syncLog.markCompleted();

      logger.info('Webhook event processed successfully', {
        eventType,
        eventId,
        result
      });

      return result;

    } catch (error) {
      logger.error('Webhook event processing error', {
        eventType,
        eventId,
        error: error.message
      });

      await syncLog.markFailed(error);
      throw error;
    }
  }

  /**
   * Handle cap table updated event
   */
  async handleCapTableUpdated(connection, data, eventType, syncLog) {
    try {
      const { cap_table_id, company_id, snapshot_date } = data;

      logger.info('Processing cap table update', {
        capTableId: cap_table_id,
        companyId: company_id
      });

      // Fetch latest cap table data from Carta
      const cartaCapTable = await cartaApiClient.getCapTable(
        connection.organizationId,
        company_id
      );

      // Find existing cap table
      let capTable = await CapTable.findOne({
        'cartaIntegration.cartaCapTableId': cap_table_id
      });

      // Map Carta data to Flora schema
      const mappedData = await cartaDataMapper.mapCapTable(cartaCapTable);

      if (capTable) {
        // Update existing cap table
        Object.assign(capTable, mappedData);
        capTable.cartaIntegration.lastCartaSync = new Date();
        capTable.cartaIntegration.cartaSyncStatus = 'synced';
      } else {
        // Create new cap table
        capTable = new CapTable({
          ...mappedData,
          fundId: connection.organizationId,
          cartaIntegration: {
            cartaCapTableId: cap_table_id,
            lastCartaSync: new Date(),
            cartaSyncStatus: 'synced',
            isSyncEnabled: true,
            cartaMetadata: cartaCapTable
          }
        });
      }

      await capTable.save();

      await syncLog.addOperation({
        entityType: 'capTable',
        entityId: cap_table_id,
        floraEntityId: capTable._id,
        operation: capTable.isNew ? 'create' : 'update',
        status: 'success'
      });

      return { success: true, synced: 1, failed: 0 };

    } catch (error) {
      logger.error('Failed to handle cap table update', { error: error.message });
      return { success: false, synced: 0, failed: 1, error: error.message };
    }
  }

  /**
   * Handle stakeholder created event
   */
  async handleStakeholderCreated(connection, data, eventType, syncLog) {
    try {
      const { stakeholder_id } = data;

      logger.info('Processing stakeholder creation', { stakeholderId: stakeholder_id });

      // Fetch stakeholder data from Carta
      const cartaStakeholder = await cartaApiClient.getStakeholder(
        connection.organizationId,
        stakeholder_id
      );

      // Map to Flora schema
      const mappedData = await cartaDataMapper.mapStakeholder(cartaStakeholder);

      // Create new stakeholder
      const stakeholder = new Stakeholder({
        ...mappedData,
        cartaIntegration: {
          cartaStakeholderId: stakeholder_id,
          lastCartaSync: new Date(),
          cartaSyncStatus: 'synced',
          isSyncEnabled: true,
          cartaMetadata: cartaStakeholder
        }
      });

      await stakeholder.save();

      await syncLog.addOperation({
        entityType: 'stakeholder',
        entityId: stakeholder_id,
        floraEntityId: stakeholder._id,
        operation: 'create',
        status: 'success'
      });

      return { success: true, synced: 1, failed: 0 };

    } catch (error) {
      logger.error('Failed to handle stakeholder creation', { error: error.message });
      return { success: false, synced: 0, failed: 1, error: error.message };
    }
  }

  /**
   * Handle stakeholder updated event
   */
  async handleStakeholderUpdated(connection, data, eventType, syncLog) {
    try {
      const { stakeholder_id } = data;

      logger.info('Processing stakeholder update', { stakeholderId: stakeholder_id });

      // Find existing stakeholder
      const stakeholder = await Stakeholder.findOne({
        'cartaIntegration.cartaStakeholderId': stakeholder_id
      });

      if (!stakeholder) {
        logger.warn('Stakeholder not found, creating new', { stakeholderId: stakeholder_id });
        return await this.handleStakeholderCreated(connection, data, eventType, syncLog);
      }

      // Fetch latest data
      const cartaStakeholder = await cartaApiClient.getStakeholder(
        connection.organizationId,
        stakeholder_id
      );

      // Map and update
      const mappedData = await cartaDataMapper.mapStakeholder(cartaStakeholder);
      Object.assign(stakeholder, mappedData);
      stakeholder.cartaIntegration.lastCartaSync = new Date();
      stakeholder.cartaIntegration.cartaSyncStatus = 'synced';

      await stakeholder.save();

      await syncLog.addOperation({
        entityType: 'stakeholder',
        entityId: stakeholder_id,
        floraEntityId: stakeholder._id,
        operation: 'update',
        status: 'success'
      });

      return { success: true, synced: 1, failed: 0 };

    } catch (error) {
      logger.error('Failed to handle stakeholder update', { error: error.message });
      return { success: false, synced: 0, failed: 1, error: error.message };
    }
  }

  /**
   * Handle stakeholder deleted event
   */
  async handleStakeholderDeleted(connection, data, eventType, syncLog) {
    try {
      const { stakeholder_id } = data;

      logger.info('Processing stakeholder deletion', { stakeholderId: stakeholder_id });

      const stakeholder = await Stakeholder.findOne({
        'cartaIntegration.cartaStakeholderId': stakeholder_id
      });

      if (stakeholder) {
        // Soft delete or mark as deleted
        stakeholder.cartaIntegration.cartaSyncStatus = 'deleted_in_source';
        stakeholder.isActive = false;
        await stakeholder.save();

        await syncLog.addOperation({
          entityType: 'stakeholder',
          entityId: stakeholder_id,
          floraEntityId: stakeholder._id,
          operation: 'delete',
          status: 'success'
        });
      }

      return { success: true, synced: 1, failed: 0 };

    } catch (error) {
      logger.error('Failed to handle stakeholder deletion', { error: error.message });
      return { success: false, synced: 0, failed: 1, error: error.message };
    }
  }

  /**
   * Handle investment created event
   */
  async handleInvestmentCreated(connection, data, eventType, syncLog) {
    try {
      const { investment_id, company_id } = data;

      logger.info('Processing investment creation', {
        investmentId: investment_id,
        companyId: company_id
      });

      // Fetch investment data
      const cartaInvestment = await cartaApiClient.getInvestment(
        connection.organizationId,
        investment_id
      );

      // Map to Flora schema
      const mappedData = await cartaDataMapper.mapInvestment(cartaInvestment);

      // Create new investment
      const investment = new Investment({
        ...mappedData,
        fundId: connection.organizationId,
        cartaIntegration: {
          cartaInvestmentId: investment_id,
          cartaCompanyId: company_id,
          lastCartaSync: new Date(),
          cartaSyncStatus: 'synced',
          isSyncEnabled: true,
          cartaMetadata: cartaInvestment
        }
      });

      await investment.save();

      await syncLog.addOperation({
        entityType: 'investment',
        entityId: investment_id,
        floraEntityId: investment._id,
        operation: 'create',
        status: 'success'
      });

      return { success: true, synced: 1, failed: 0 };

    } catch (error) {
      logger.error('Failed to handle investment creation', { error: error.message });
      return { success: false, synced: 0, failed: 1, error: error.message };
    }
  }

  /**
   * Handle investment updated event
   */
  async handleInvestmentUpdated(connection, data, eventType, syncLog) {
    try {
      const { investment_id } = data;

      logger.info('Processing investment update', { investmentId: investment_id });

      const investment = await Investment.findOne({
        'cartaIntegration.cartaInvestmentId': investment_id
      });

      if (!investment) {
        return await this.handleInvestmentCreated(connection, data, eventType, syncLog);
      }

      const cartaInvestment = await cartaApiClient.getInvestment(
        connection.organizationId,
        investment_id
      );

      const mappedData = await cartaDataMapper.mapInvestment(cartaInvestment);
      Object.assign(investment, mappedData);
      investment.cartaIntegration.lastCartaSync = new Date();
      investment.cartaIntegration.cartaSyncStatus = 'synced';

      await investment.save();

      await syncLog.addOperation({
        entityType: 'investment',
        entityId: investment_id,
        floraEntityId: investment._id,
        operation: 'update',
        status: 'success'
      });

      return { success: true, synced: 1, failed: 0 };

    } catch (error) {
      logger.error('Failed to handle investment update', { error: error.message });
      return { success: false, synced: 0, failed: 1, error: error.message };
    }
  }

  /**
   * Handle valuation updated event
   */
  async handleValuationUpdated(connection, data, eventType, syncLog) {
    try {
      const { valuation_id, company_id } = data;

      logger.info('Processing valuation update', {
        valuationId: valuation_id,
        companyId: company_id
      });

      // Find related investment
      const investment = await Investment.findOne({
        'cartaIntegration.cartaCompanyId': company_id
      });

      if (!investment) {
        logger.warn('No investment found for valuation update', { companyId: company_id });
        return { success: false, synced: 0, failed: 1, message: 'Investment not found' };
      }

      // Fetch valuation data
      const cartaValuation = await cartaApiClient.getValuation(
        connection.organizationId,
        valuation_id
      );

      // Map valuation
      const mappedValuation = await cartaDataMapper.mapValuation(cartaValuation);

      // Update investment valuation
      investment.currentValuation = mappedValuation;
      investment.valuations = investment.valuations || [];
      investment.valuations.push(mappedValuation);

      await investment.save();

      await syncLog.addOperation({
        entityType: 'valuation',
        entityId: valuation_id,
        floraEntityId: investment._id,
        operation: 'update',
        status: 'success'
      });

      return { success: true, synced: 1, failed: 0 };

    } catch (error) {
      logger.error('Failed to handle valuation update', { error: error.message });
      return { success: false, synced: 0, failed: 1, error: error.message };
    }
  }

  /**
   * Handle transaction created event
   */
  async handleTransactionCreated(connection, data, eventType, syncLog) {
    try {
      const { transaction_id, company_id } = data;

      logger.info('Processing transaction event', {
        transactionId: transaction_id,
        companyId: company_id
      });

      // Queue a cap table sync for the affected company
      await cartaSyncService.syncQueue.add('sync-cap-table', {
        connectionId: connection._id,
        entityId: company_id
      }, {
        priority: 2
      });

      return { success: true, synced: 1, failed: 0, message: 'Cap table sync queued' };

    } catch (error) {
      logger.error('Failed to handle transaction event', { error: error.message });
      return { success: false, synced: 0, failed: 1, error: error.message };
    }
  }

  /**
   * Handle fund updated event
   */
  async handleFundUpdated(connection, data, eventType, syncLog) {
    try {
      const { fund_id } = data;

      logger.info('Processing fund update', { fundId: fund_id });

      const fund = await Fund.findById(connection.organizationId);

      if (!fund) {
        logger.warn('Fund not found', { fundId: connection.organizationId });
        return { success: false, synced: 0, failed: 1, message: 'Fund not found' };
      }

      // Fetch latest fund data
      const cartaFund = await cartaApiClient.getFund(
        connection.organizationId,
        fund_id
      );

      // Map and update
      const mappedData = await cartaDataMapper.mapFund(cartaFund);
      Object.assign(fund, mappedData);
      fund.cartaIntegration.lastCartaSync = new Date();
      fund.cartaIntegration.cartaSyncStatus = 'synced';

      await fund.save();

      await syncLog.addOperation({
        entityType: 'fund',
        entityId: fund_id,
        floraEntityId: fund._id,
        operation: 'update',
        status: 'success'
      });

      return { success: true, synced: 1, failed: 0 };

    } catch (error) {
      logger.error('Failed to handle fund update', { error: error.message });
      return { success: false, synced: 0, failed: 1, error: error.message };
    }
  }

  /**
   * Handle capital call events
   */
  async handleCapitalCallEvent(connection, data, eventType, syncLog) {
    try {
      const { capital_call_id, fund_id } = data;

      logger.info('Processing capital call event', {
        capitalCallId: capital_call_id,
        fundId: fund_id,
        eventType
      });

      // Queue a sync for capital calls
      await cartaSyncService.syncQueue.add('sync-capital-calls', {
        connectionId: connection._id,
        fundId: fund_id
      }, {
        priority: 3
      });

      return { success: true, synced: 1, failed: 0, message: 'Capital call sync queued' };

    } catch (error) {
      logger.error('Failed to handle capital call event', { error: error.message });
      return { success: false, synced: 0, failed: 1, error: error.message };
    }
  }

  /**
   * Handle distribution events
   */
  async handleDistributionEvent(connection, data, eventType, syncLog) {
    try {
      const { distribution_id, fund_id } = data;

      logger.info('Processing distribution event', {
        distributionId: distribution_id,
        fundId: fund_id,
        eventType
      });

      // Queue a sync for distributions
      await cartaSyncService.syncQueue.add('sync-distributions', {
        connectionId: connection._id,
        fundId: fund_id
      }, {
        priority: 3
      });

      return { success: true, synced: 1, failed: 0, message: 'Distribution sync queued' };

    } catch (error) {
      logger.error('Failed to handle distribution event', { error: error.message });
      return { success: false, synced: 0, failed: 1, error: error.message };
    }
  }

  /**
   * Get event priority for queue processing
   * @param {String} eventType - Webhook event type
   * @returns {Number} Priority (1 = highest, 10 = lowest)
   */
  getEventPriority(eventType) {
    const priorityMap = {
      'cap_table.updated': 2,
      'cap_table.snapshot_created': 2,
      'stakeholder.created': 3,
      'stakeholder.updated': 3,
      'stakeholder.deleted': 3,
      'investment.created': 2,
      'investment.updated': 2,
      'valuation.updated': 4,
      'valuation.created': 4,
      'transaction.created': 2,
      'transaction.updated': 2,
      'fund.updated': 1,
      'capital_call.created': 1,
      'capital_call.updated': 1,
      'distribution.created': 1,
      'distribution.updated': 1
    };

    return priorityMap[eventType] || 5;
  }

  /**
   * Register webhook with Carta
   * @param {Object} connection - Connection instance
   * @param {Array} events - Event types to subscribe to
   * @returns {Promise<Object>} Webhook registration result
   */
  async registerWebhook(connection, events) {
    try {
      const webhookUrl = `${process.env.APP_URL}/api/v1/integrations/carta/webhook`;

      const config = {
        url: webhookUrl,
        events: events,
        description: 'Flora Carta Integration Webhook',
        active: true
      };

      const result = await cartaApiClient.registerWebhook(
        connection.organizationId,
        config
      );

      // Update connection with webhook info
      connection.webhookSettings = {
        enabled: true,
        webhookId: result.id,
        subscribedEvents: events
      };

      await connection.save();

      logger.info('Webhook registered successfully', {
        connectionId: connection._id,
        webhookId: result.id,
        events
      });

      return {
        success: true,
        webhookId: result.id,
        url: webhookUrl,
        events
      };

    } catch (error) {
      logger.error('Failed to register webhook', {
        connectionId: connection._id,
        error: error.message
      });
      throw error;
    }
  }

  /**
   * Unregister webhook from Carta
   * @param {Object} connection - Connection instance
   * @returns {Promise<Object>} Unregistration result
   */
  async unregisterWebhook(connection) {
    try {
      if (!connection.webhookSettings?.webhookId) {
        logger.info('No webhook to unregister', { connectionId: connection._id });
        return { success: true, message: 'No webhook registered' };
      }

      await cartaApiClient.deleteWebhook(
        connection.organizationId,
        connection.webhookSettings.webhookId
      );

      // Clear webhook settings
      connection.webhookSettings = {
        enabled: false,
        webhookId: null,
        subscribedEvents: []
      };

      await connection.save();

      logger.info('Webhook unregistered successfully', {
        connectionId: connection._id
      });

      return { success: true, message: 'Webhook unregistered' };

    } catch (error) {
      logger.error('Failed to unregister webhook', {
        connectionId: connection._id,
        error: error.message
      });
      throw error;
    }
  }

  /**
   * Get webhook statistics
   * @param {String} connectionId - Connection ID
   * @param {Object} dateRange - Date range for statistics
   * @returns {Promise<Object>} Webhook statistics
   */
  async getWebhookStatistics(connectionId, dateRange = {}) {
    try {
      const query = {
        connectionId,
        syncType: 'webhook'
      };

      if (dateRange.start) {
        query.createdAt = { $gte: new Date(dateRange.start) };
      }

      if (dateRange.end) {
        query.createdAt = query.createdAt || {};
        query.createdAt.$lte = new Date(dateRange.end);
      }

      const logs = await CartaSyncLog.find(query);

      const stats = {
        totalEvents: logs.length,
        successfulEvents: logs.filter(l => l.status === 'completed').length,
        failedEvents: logs.filter(l => l.status === 'failed').length,
        eventsByType: {},
        averageProcessingTime: 0
      };

      // Calculate event type distribution
      logs.forEach(log => {
        const eventType = log.trigger?.webhookEvent;
        if (eventType) {
          stats.eventsByType[eventType] = (stats.eventsByType[eventType] || 0) + 1;
        }
      });

      // Calculate average processing time
      const completedLogs = logs.filter(l => l.completedAt && l.startedAt);
      if (completedLogs.length > 0) {
        const totalTime = completedLogs.reduce((sum, log) => {
          return sum + (new Date(log.completedAt) - new Date(log.startedAt));
        }, 0);
        stats.averageProcessingTime = Math.round(totalTime / completedLogs.length);
      }

      return stats;

    } catch (error) {
      logger.error('Failed to get webhook statistics', {
        connectionId,
        error: error.message
      });
      throw error;
    }
  }
}

module.exports = new CartaWebhookService();
