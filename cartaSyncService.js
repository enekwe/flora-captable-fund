const Bull = require('bull');
const CartaConnection = require('../../models/carta/CartaConnection');
const CartaSyncLog = require('../../models/carta/CartaSyncLog');
const CartaConflict = require('../../models/carta/CartaConflict');
const cartaApiClient = require('./cartaApiClient');
const cartaDataMapper = require('./cartaDataMapper');
const cartaConflictResolver = require('./cartaConflictResolver');
const Fund = require('../../models/Fund');
const Investment = require('../../models/Investment');
const Stakeholder = require('../../models/Stakeholder');
const CapTable = require('../../models/CapTable');
const logger = require('../../utils/logger');

/**
 * CartaSyncService
 * Main orchestration service for syncing data between Carta and Flora
 * Handles full sync, incremental sync, and entity-specific synchronization
 */
class CartaSyncService {
  constructor() {
    // Initialize Bull queue for async sync jobs
    this.syncQueue = new Bull('carta-sync', {
      redis: {
        host: process.env.REDIS_HOST || 'localhost',
        port: process.env.REDIS_PORT || 6379,
        password: process.env.REDIS_PASSWORD
      },
      defaultJobOptions: {
        attempts: 3,
        backoff: {
          type: 'exponential',
          delay: 2000
        },
        removeOnComplete: 100,
        removeOnFail: false
      }
    });

    this.setupQueueProcessors();
  }

  /**
   * Setup queue processors for different sync job types
   */
  setupQueueProcessors() {
    // Full sync processor
    this.syncQueue.process('full-sync', async (job) => {
      return await this.processFullSync(job.data);
    });

    // Incremental sync processor
    this.syncQueue.process('incremental-sync', async (job) => {
      return await this.processIncrementalSync(job.data);
    });

    // Entity-specific sync processors
    this.syncQueue.process('sync-cap-table', async (job) => {
      return await this.syncCapTable(job.data.connectionId, job.data.entityId);
    });

    this.syncQueue.process('sync-funds', async (job) => {
      return await this.syncFunds(job.data.connectionId);
    });

    this.syncQueue.process('sync-investments', async (job) => {
      return await this.syncInvestments(job.data.connectionId);
    });

    this.syncQueue.process('sync-stakeholders', async (job) => {
      return await this.syncStakeholders(job.data.connectionId);
    });

    // Queue event handlers
    this.syncQueue.on('completed', (job, result) => {
      logger.info('Carta sync job completed', {
        jobId: job.id,
        jobType: job.name,
        result
      });
    });

    this.syncQueue.on('failed', (job, err) => {
      logger.error('Carta sync job failed', {
        jobId: job.id,
        jobType: job.name,
        error: err.message,
        stack: err.stack
      });
    });
  }

  /**
   * Queue a full sync operation
   * @param {String} fundId - Flora fund ID
   * @param {String} userId - User initiating sync
   * @returns {Promise} Job promise
   */
  async queueFullSync(fundId, userId) {
    const connection = await CartaConnection.findActiveByFund(fundId);

    if (!connection) {
      throw new Error('No active Carta connection found for fund');
    }

    const job = await this.syncQueue.add('full-sync', {
      connectionId: connection._id,
      fundId,
      userId,
      type: 'full'
    }, {
      priority: 1
    });

    logger.info('Full sync queued', {
      jobId: job.id,
      fundId,
      connectionId: connection._id
    });

    return job;
  }

  /**
   * Queue an incremental sync operation
   * @param {String} fundId - Flora fund ID
   * @returns {Promise} Job promise
   */
  async queueIncrementalSync(fundId) {
    const connection = await CartaConnection.findActiveByFund(fundId);

    if (!connection) {
      throw new Error('No active Carta connection found for fund');
    }

    const job = await this.syncQueue.add('incremental-sync', {
      connectionId: connection._id,
      fundId,
      type: 'incremental'
    }, {
      priority: 2
    });

    return job;
  }

  /**
   * Process full sync operation
   * @param {Object} data - Job data
   * @returns {Promise<Object>} Sync results
   */
  async processFullSync(data) {
    const { connectionId, fundId, userId } = data;
    const startTime = Date.now();

    // Create sync log
    const syncLog = new CartaSyncLog({
      connectionId,
      fundId,
      syncType: 'full',
      syncDirection: 'carta_to_flora',
      status: 'in_progress',
      trigger: {
        type: 'manual',
        initiatedBy: userId
      }
    });

    await syncLog.save();

    try {
      // Get connection with tokens
      const connection = await CartaConnection.findById(connectionId)
        .select('+oauth.accessToken +oauth.refreshToken');

      if (!connection) {
        throw new Error('Connection not found');
      }

      // Check if token needs refresh
      if (connection.needsRefresh) {
        await this.refreshConnectionToken(connection);
      }

      // Initialize results
      const results = {
        capTables: { synced: 0, failed: 0 },
        funds: { synced: 0, failed: 0 },
        investments: { synced: 0, failed: 0 },
        stakeholders: { synced: 0, failed: 0 },
        valuations: { synced: 0, failed: 0 }
      };

      // Sync entities in order (stakeholders first, then funds, then investments, then cap tables)
      if (connection.syncConfig.entities.stakeholders) {
        logger.info('Syncing stakeholders', { connectionId });
        const stakeholderResults = await this.syncStakeholders(connectionId, syncLog);
        results.stakeholders = stakeholderResults;
      }

      if (connection.syncConfig.entities.funds) {
        logger.info('Syncing funds', { connectionId });
        const fundResults = await this.syncFunds(connectionId, syncLog);
        results.funds = fundResults;
      }

      if (connection.syncConfig.entities.investments) {
        logger.info('Syncing investments', { connectionId });
        const investmentResults = await this.syncInvestments(connectionId, syncLog);
        results.investments = investmentResults;
      }

      if (connection.syncConfig.entities.capTable) {
        logger.info('Syncing cap tables', { connectionId });
        const capTableResults = await this.syncCapTables(connectionId, syncLog);
        results.capTables = capTableResults;
      }

      if (connection.syncConfig.entities.valuations) {
        logger.info('Syncing valuations', { connectionId });
        const valuationResults = await this.syncValuations(connectionId, syncLog);
        results.valuations = valuationResults;
      }

      // Update sync log
      syncLog.summary.totalSynced = Object.values(results).reduce((sum, r) => sum + (r.synced || 0), 0);
      syncLog.summary.totalFailed = Object.values(results).reduce((sum, r) => sum + (r.failed || 0), 0);

      await syncLog.markCompleted();

      // Update connection statistics
      const duration = Date.now() - startTime;
      await connection.recordSyncComplete(true, duration, {
        stakeholders: results.stakeholders.synced,
        funds: results.funds.synced,
        investments: results.investments.synced,
        capTables: results.capTables.synced,
        valuations: results.valuations.synced
      });

      // Update last full sync timestamp
      connection.syncConfig.lastFullSync = new Date();
      await connection.save();

      logger.info('Full sync completed', {
        connectionId,
        fundId,
        duration,
        results
      });

      return {
        success: true,
        syncLogId: syncLog._id,
        duration,
        results
      };

    } catch (error) {
      logger.error('Full sync failed', {
        connectionId,
        fundId,
        error: error.message,
        stack: error.stack
      });

      await syncLog.markFailed(error);

      const connection = await CartaConnection.findById(connectionId);
      if (connection) {
        await connection.recordSyncComplete(false, Date.now() - startTime);
      }

      throw error;
    }
  }

  /**
   * Process incremental sync operation (only changed entities)
   * @param {Object} data - Job data
   * @returns {Promise<Object>} Sync results
   */
  async processIncrementalSync(data) {
    const { connectionId, fundId } = data;
    const startTime = Date.now();

    // Create sync log
    const syncLog = new CartaSyncLog({
      connectionId,
      fundId,
      syncType: 'incremental',
      syncDirection: 'carta_to_flora',
      status: 'in_progress',
      trigger: {
        type: 'scheduled'
      }
    });

    await syncLog.save();

    try {
      const connection = await CartaConnection.findById(connectionId)
        .select('+oauth.accessToken +oauth.refreshToken');

      if (!connection) {
        throw new Error('Connection not found');
      }

      // Get last sync timestamp
      const lastSync = connection.syncConfig.lastIncrementalSync ||
                       connection.syncConfig.lastFullSync ||
                       new Date(0);

      // Fetch changes from Carta since last sync
      const changes = await cartaApiClient.getChangesSince(connection, lastSync);

      const results = {
        synced: 0,
        failed: 0,
        skipped: 0
      };

      // Process each changed entity
      for (const change of changes) {
        try {
          await this.processEntityChange(connection, change, syncLog);
          results.synced++;
        } catch (error) {
          logger.error('Failed to process entity change', {
            change,
            error: error.message
          });
          results.failed++;

          await syncLog.addError({
            code: 'ENTITY_SYNC_FAILED',
            message: error.message,
            entityType: change.entityType,
            entityId: change.entityId,
            severity: 'medium'
          });
        }
      }

      // Update sync log
      syncLog.summary.totalSynced = results.synced;
      syncLog.summary.totalFailed = results.failed;
      syncLog.summary.totalSkipped = results.skipped;

      await syncLog.markCompleted();

      // Update connection
      const duration = Date.now() - startTime;
      await connection.recordSyncComplete(true, duration);
      connection.syncConfig.lastIncrementalSync = new Date();
      await connection.save();

      logger.info('Incremental sync completed', {
        connectionId,
        fundId,
        duration,
        results
      });

      return {
        success: true,
        syncLogId: syncLog._id,
        duration,
        results
      };

    } catch (error) {
      logger.error('Incremental sync failed', {
        connectionId,
        fundId,
        error: error.message
      });

      await syncLog.markFailed(error);

      throw error;
    }
  }

  /**
   * Sync stakeholders from Carta
   * @param {String} connectionId - Connection ID
   * @param {Object} syncLog - Sync log instance
   * @returns {Promise<Object>} Sync results
   */
  async syncStakeholders(connectionId, syncLog) {
    const connection = await CartaConnection.findById(connectionId)
      .select('+oauth.accessToken');

    const results = { synced: 0, failed: 0, skipped: 0 };

    try {
      // Fetch stakeholders from Carta
      const cartaStakeholders = await cartaApiClient.getStakeholders(connection);

      for (const cartaStakeholder of cartaStakeholders) {
        try {
          // Check if stakeholder already exists
          let stakeholder = await Stakeholder.findOne({
            'cartaIntegration.cartaStakeholderId': cartaStakeholder.id
          });

          // Map Carta data to Flora schema
          const mappedData = await cartaDataMapper.mapStakeholder(cartaStakeholder);

          if (stakeholder) {
            // Check for conflicts
            const hasConflict = await this.detectConflicts(stakeholder, mappedData);

            if (hasConflict) {
              // Create conflict record
              await cartaConflictResolver.createConflict({
                connectionId,
                syncLogId: syncLog._id,
                fundId: connection.fundId,
                entityType: 'stakeholder',
                cartaId: cartaStakeholder.id,
                floraId: stakeholder._id,
                cartaData: cartaStakeholder,
                floraData: stakeholder.toObject(),
                mappedData
              });

              results.skipped++;
              continue;
            }

            // Update existing stakeholder
            Object.assign(stakeholder, mappedData);
            stakeholder.cartaIntegration.lastCartaSync = new Date();
            stakeholder.cartaIntegration.cartaSyncStatus = 'synced';

            await stakeholder.save();
            results.synced++;

            if (syncLog) {
              await syncLog.addOperation({
                entityType: 'stakeholder',
                entityId: cartaStakeholder.id,
                floraEntityId: stakeholder._id,
                operation: 'update',
                status: 'success'
              });
            }

          } else {
            // Create new stakeholder
            stakeholder = new Stakeholder({
              ...mappedData,
              cartaIntegration: {
                cartaStakeholderId: cartaStakeholder.id,
                lastCartaSync: new Date(),
                cartaSyncStatus: 'synced',
                isSyncEnabled: true,
                cartaMetadata: cartaStakeholder
              }
            });

            await stakeholder.save();
            results.synced++;

            if (syncLog) {
              await syncLog.addOperation({
                entityType: 'stakeholder',
                entityId: cartaStakeholder.id,
                floraEntityId: stakeholder._id,
                operation: 'create',
                status: 'success'
              });
            }
          }

        } catch (error) {
          logger.error('Failed to sync stakeholder', {
            cartaStakeholderId: cartaStakeholder.id,
            error: error.message
          });

          results.failed++;

          if (syncLog) {
            await syncLog.addError({
              code: 'STAKEHOLDER_SYNC_ERROR',
              message: error.message,
              entityType: 'stakeholder',
              entityId: cartaStakeholder.id,
              severity: 'medium'
            });
          }
        }
      }

    } catch (error) {
      logger.error('Failed to fetch stakeholders from Carta', {
        connectionId,
        error: error.message
      });
      throw error;
    }

    return results;
  }

  /**
   * Sync funds from Carta
   * @param {String} connectionId - Connection ID
   * @param {Object} syncLog - Sync log instance
   * @returns {Promise<Object>} Sync results
   */
  async syncFunds(connectionId, syncLog) {
    const connection = await CartaConnection.findById(connectionId)
      .select('+oauth.accessToken');

    const results = { synced: 0, failed: 0, skipped: 0 };

    try {
      const cartaFunds = await cartaApiClient.getFunds(connection);

      for (const cartaFund of cartaFunds) {
        try {
          let fund = await Fund.findById(connection.fundId);

          const mappedData = await cartaDataMapper.mapFund(cartaFund);

          // Check for conflicts
          const hasConflict = await this.detectConflicts(fund, mappedData);

          if (hasConflict) {
            await cartaConflictResolver.createConflict({
              connectionId,
              syncLogId: syncLog._id,
              fundId: connection.fundId,
              entityType: 'fund',
              cartaId: cartaFund.id,
              floraId: fund._id,
              cartaData: cartaFund,
              floraData: fund.toObject(),
              mappedData
            });

            results.skipped++;
            continue;
          }

          // Update fund with Carta data
          Object.assign(fund, mappedData);
          fund.cartaIntegration = {
            cartaFundId: cartaFund.id,
            cartaOrgId: connection.cartaOrgId,
            lastCartaSync: new Date(),
            cartaSyncStatus: 'synced',
            isSyncEnabled: true,
            cartaMetadata: cartaFund
          };

          await fund.save();
          results.synced++;

          if (syncLog) {
            await syncLog.addOperation({
              entityType: 'fund',
              entityId: cartaFund.id,
              floraEntityId: fund._id,
              operation: 'update',
              status: 'success'
            });
          }

        } catch (error) {
          logger.error('Failed to sync fund', {
            cartaFundId: cartaFund.id,
            error: error.message
          });
          results.failed++;
        }
      }

    } catch (error) {
      logger.error('Failed to fetch funds from Carta', {
        connectionId,
        error: error.message
      });
      throw error;
    }

    return results;
  }

  /**
   * Sync investments from Carta
   * @param {String} connectionId - Connection ID
   * @param {Object} syncLog - Sync log instance
   * @returns {Promise<Object>} Sync results
   */
  async syncInvestments(connectionId, syncLog) {
    const connection = await CartaConnection.findById(connectionId)
      .select('+oauth.accessToken');

    const results = { synced: 0, failed: 0, skipped: 0 };

    try {
      const cartaInvestments = await cartaApiClient.getInvestments(connection);

      for (const cartaInvestment of cartaInvestments) {
        try {
          let investment = await Investment.findOne({
            'cartaIntegration.cartaInvestmentId': cartaInvestment.id
          });

          const mappedData = await cartaDataMapper.mapInvestment(cartaInvestment);

          if (investment) {
            // Check for conflicts
            const hasConflict = await this.detectConflicts(investment, mappedData);

            if (hasConflict) {
              await cartaConflictResolver.createConflict({
                connectionId,
                syncLogId: syncLog._id,
                fundId: connection.fundId,
                entityType: 'investment',
                cartaId: cartaInvestment.id,
                floraId: investment._id,
                cartaData: cartaInvestment,
                floraData: investment.toObject(),
                mappedData
              });

              results.skipped++;
              continue;
            }

            Object.assign(investment, mappedData);
            investment.cartaIntegration.lastCartaSync = new Date();
            investment.cartaIntegration.cartaSyncStatus = 'synced';

            await investment.save();
            results.synced++;

          } else {
            investment = new Investment({
              ...mappedData,
              fundId: connection.fundId,
              cartaIntegration: {
                cartaInvestmentId: cartaInvestment.id,
                cartaCompanyId: cartaInvestment.companyId,
                lastCartaSync: new Date(),
                cartaSyncStatus: 'synced',
                isSyncEnabled: true,
                cartaMetadata: cartaInvestment
              }
            });

            await investment.save();
            results.synced++;
          }

          if (syncLog) {
            await syncLog.addOperation({
              entityType: 'investment',
              entityId: cartaInvestment.id,
              floraEntityId: investment._id,
              operation: investment.isNew ? 'create' : 'update',
              status: 'success'
            });
          }

        } catch (error) {
          logger.error('Failed to sync investment', {
            cartaInvestmentId: cartaInvestment.id,
            error: error.message
          });
          results.failed++;
        }
      }

    } catch (error) {
      logger.error('Failed to fetch investments from Carta', {
        connectionId,
        error: error.message
      });
      throw error;
    }

    return results;
  }

  /**
   * Sync cap tables from Carta
   * @param {String} connectionId - Connection ID
   * @param {Object} syncLog - Sync log instance
   * @returns {Promise<Object>} Sync results
   */
  async syncCapTables(connectionId, syncLog) {
    const connection = await CartaConnection.findById(connectionId)
      .select('+oauth.accessToken');

    const results = { synced: 0, failed: 0, skipped: 0 };

    try {
      const cartaCapTables = await cartaApiClient.getCapTables(connection);

      for (const cartaCapTable of cartaCapTables) {
        try {
          let capTable = await CapTable.findOne({
            'cartaIntegration.cartaCapTableId': cartaCapTable.id
          });

          const mappedData = await cartaDataMapper.mapCapTable(cartaCapTable);

          if (capTable) {
            const hasConflict = await this.detectConflicts(capTable, mappedData);

            if (hasConflict) {
              await cartaConflictResolver.createConflict({
                connectionId,
                syncLogId: syncLog._id,
                fundId: connection.fundId,
                entityType: 'capTable',
                cartaId: cartaCapTable.id,
                floraId: capTable._id,
                cartaData: cartaCapTable,
                floraData: capTable.toObject(),
                mappedData
              });

              results.skipped++;
              continue;
            }

            Object.assign(capTable, mappedData);
            capTable.cartaIntegration.lastCartaSync = new Date();
            capTable.cartaIntegration.cartaSyncStatus = 'synced';

            await capTable.save();
            results.synced++;

          } else {
            capTable = new CapTable({
              ...mappedData,
              fundId: connection.fundId,
              cartaIntegration: {
                cartaCapTableId: cartaCapTable.id,
                lastCartaSync: new Date(),
                cartaSyncStatus: 'synced',
                isSyncEnabled: true,
                cartaMetadata: cartaCapTable
              }
            });

            await capTable.save();
            results.synced++;
          }

          if (syncLog) {
            await syncLog.addOperation({
              entityType: 'capTable',
              entityId: cartaCapTable.id,
              floraEntityId: capTable._id,
              operation: capTable.isNew ? 'create' : 'update',
              status: 'success'
            });
          }

        } catch (error) {
          logger.error('Failed to sync cap table', {
            cartaCapTableId: cartaCapTable.id,
            error: error.message
          });
          results.failed++;
        }
      }

    } catch (error) {
      logger.error('Failed to fetch cap tables from Carta', {
        connectionId,
        error: error.message
      });
      throw error;
    }

    return results;
  }

  /**
   * Sync valuations from Carta
   * @param {String} connectionId - Connection ID
   * @param {Object} syncLog - Sync log instance
   * @returns {Promise<Object>} Sync results
   */
  async syncValuations(connectionId, syncLog) {
    const results = { synced: 0, failed: 0, skipped: 0 };

    try {
      const connection = await CartaConnection.findById(connectionId)
        .select('+oauth.accessToken');

      const cartaValuations = await cartaApiClient.getValuations(connection);

      // Process each valuation (typically updating investment records)
      for (const cartaValuation of cartaValuations) {
        try {
          // Find related investment
          const investment = await Investment.findOne({
            'cartaIntegration.cartaCompanyId': cartaValuation.companyId
          });

          if (investment) {
            // Update valuation data
            const mappedValuation = await cartaDataMapper.mapValuation(cartaValuation);

            investment.valuations = investment.valuations || [];
            investment.valuations.push(mappedValuation);

            // Update current valuation
            if (cartaValuation.isCurrent) {
              investment.currentValuation = mappedValuation;
            }

            await investment.save();
            results.synced++;

            if (syncLog) {
              await syncLog.addOperation({
                entityType: 'valuation',
                entityId: cartaValuation.id,
                floraEntityId: investment._id,
                operation: 'update',
                status: 'success'
              });
            }
          } else {
            results.skipped++;
          }

        } catch (error) {
          logger.error('Failed to sync valuation', {
            cartaValuationId: cartaValuation.id,
            error: error.message
          });
          results.failed++;
        }
      }

    } catch (error) {
      logger.error('Failed to fetch valuations from Carta', {
        connectionId,
        error: error.message
      });
      throw error;
    }

    return results;
  }

  /**
   * Process a single entity change (for incremental sync)
   * @param {Object} connection - Connection instance
   * @param {Object} change - Change event from Carta
   * @param {Object} syncLog - Sync log instance
   */
  async processEntityChange(connection, change, syncLog) {
    const { entityType, entityId, changeType, data } = change;

    logger.info('Processing entity change', {
      entityType,
      entityId,
      changeType
    });

    switch (entityType) {
      case 'stakeholder':
        return await this.processStakeholderChange(connection, entityId, changeType, data, syncLog);
      case 'fund':
        return await this.processFundChange(connection, entityId, changeType, data, syncLog);
      case 'investment':
        return await this.processInvestmentChange(connection, entityId, changeType, data, syncLog);
      case 'capTable':
        return await this.processCapTableChange(connection, entityId, changeType, data, syncLog);
      default:
        logger.warn('Unknown entity type', { entityType });
    }
  }

  /**
   * Process stakeholder change
   */
  async processStakeholderChange(connection, entityId, changeType, data, syncLog) {
    // Implementation similar to sync but for single entity
    // Handle create, update, delete based on changeType
  }

  /**
   * Detect conflicts between Carta and Flora data
   * @param {Object} floraEntity - Current Flora entity
   * @param {Object} mappedData - Mapped Carta data
   * @returns {Boolean} Has conflict
   */
  async detectConflicts(floraEntity, mappedData) {
    // Check if Flora entity has been modified since last sync
    const lastSync = floraEntity.cartaIntegration?.lastCartaSync;

    if (!lastSync) {
      return false; // First sync, no conflict
    }

    const floraModifiedAt = floraEntity.updatedAt || floraEntity.lastModifiedAt;

    if (floraModifiedAt && floraModifiedAt > lastSync) {
      // Flora entity was modified after last sync
      // Check if there are actual data differences
      return await this.hasDataDifferences(floraEntity, mappedData);
    }

    return false;
  }

  /**
   * Check if there are actual data differences
   * @param {Object} floraEntity - Flora entity
   * @param {Object} mappedData - Mapped Carta data
   * @returns {Boolean} Has differences
   */
  async hasDataDifferences(floraEntity, mappedData) {
    // Compare critical fields
    const criticalFields = this.getCriticalFieldsForEntity(floraEntity.constructor.modelName);

    for (const field of criticalFields) {
      const floraValue = this.getNestedValue(floraEntity, field);
      const cartaValue = this.getNestedValue(mappedData, field);

      if (floraValue !== cartaValue) {
        return true;
      }
    }

    return false;
  }

  /**
   * Get critical fields for conflict detection
   * @param {String} modelName - Model name
   * @returns {Array} Critical field paths
   */
  getCriticalFieldsForEntity(modelName) {
    const criticalFields = {
      Fund: ['name', 'totalCommitment', 'configuration.managementFee', 'configuration.carriedInterest'],
      Investment: ['companyName', 'initialInvestment.amount', 'currentValuation.fmv'],
      Stakeholder: ['name', 'email', 'lpDetails.commitmentAmount'],
      CapTable: ['summary.totalOutstandingShares', 'summary.fullyDilutedShares']
    };

    return criticalFields[modelName] || [];
  }

  /**
   * Get nested value from object
   * @param {Object} obj - Object
   * @param {String} path - Dot-separated path
   * @returns {*} Value
   */
  getNestedValue(obj, path) {
    return path.split('.').reduce((current, key) => current?.[key], obj);
  }

  /**
   * Refresh connection token
   * @param {Object} connection - Connection instance
   */
  async refreshConnectionToken(connection) {
    const cartaAuthService = require('./cartaAuthService');

    try {
      const newTokens = await cartaAuthService.refreshAccessToken(connection.oauth.refreshToken);

      await connection.updateTokens(
        newTokens.accessToken,
        newTokens.refreshToken,
        newTokens.expiresIn
      );

      logger.info('Connection token refreshed', {
        connectionId: connection._id
      });

    } catch (error) {
      logger.error('Failed to refresh connection token', {
        connectionId: connection._id,
        error: error.message
      });

      connection.status = 'expired';
      await connection.save();

      throw error;
    }
  }

  /**
   * Get sync status for a fund
   * @param {String} fundId - Fund ID
   * @returns {Promise<Object>} Sync status
   */
  async getSyncStatus(fundId) {
    const connection = await CartaConnection.findOne({ fundId, isDeleted: false });

    if (!connection) {
      return {
        connected: false,
        message: 'No Carta connection found'
      };
    }

    const recentSyncs = await CartaSyncLog.getRecentSyncs(fundId, 5);
    const pendingConflicts = await CartaConflict.getPendingConflicts(fundId);

    return {
      connected: true,
      status: connection.status,
      lastSync: connection.syncConfig.lastIncrementalSync,
      lastFullSync: connection.syncConfig.lastFullSync,
      nextScheduledSync: connection.syncConfig.nextScheduledSync,
      recentSyncs,
      pendingConflicts: pendingConflicts.length,
      statistics: connection.statistics
    };
  }
}

module.exports = new CartaSyncService();
