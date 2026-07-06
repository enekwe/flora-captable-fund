/**
 * Carta Conflict Resolver Service
 * Handles automated conflict detection and resolution between Carta and Flora data
 * Implements multiple resolution strategies with audit logging
 */

const CartaConflict = require('../../models/carta/CartaConflict');
const logger = require('../../utils/logger');

class CartaConflictResolver {
  /**
   * Create a new conflict record
   * @param {Object} conflictData - Conflict information
   * @returns {Promise<Object>} Created conflict record
   */
  async createConflict(conflictData) {
    try {
      const {
        connectionId,
        syncLogId,
        fundId,
        entityType,
        cartaId,
        floraId,
        cartaData,
        floraData,
        mappedData
      } = conflictData;

      // Detect field-level conflicts
      const fields = this.detectFieldConflicts(floraData, mappedData, entityType);

      // Determine conflict type
      const conflictType = this.determineConflictType(floraData, mappedData, cartaData);

      // Calculate priority based on severity
      const priority = this.calculatePriority(fields, entityType);

      // Get SLA target
      const slaTarget = this.getSLATarget(entityType, priority);

      // Create conflict record
      const conflict = new CartaConflict({
        connectionId,
        syncLogId,
        fundId,
        conflictType,
        entity: {
          type: entityType,
          cartaId,
          floraId,
          floraModel: this.getModelName(entityType),
          entityName: floraData.name || floraData.companyName || 'Unknown',
          entityDescription: this.getEntityDescription(floraData, entityType)
        },
        conflictDetails: {
          fields,
          cartaLastModifiedAt: cartaData.updated_at ? new Date(cartaData.updated_at) : new Date(),
          floraLastModifiedAt: floraData.updatedAt || new Date(),
          floraLastModifiedBy: floraData.lastModifiedBy,
          detectedAt: new Date(),
          detectionMethod: 'sync',
          conflictSummary: this.generateConflictSummary(fields)
        },
        priority,
        sla: {
          targetResolutionTime: slaTarget,
          dueDate: new Date(Date.now() + slaTarget * 60000) // Convert minutes to milliseconds
        },
        status: 'pending',
        resolution: {
          strategy: this.getDefaultStrategy(entityType, conflictType, fields)
        }
      });

      await conflict.save();

      logger.info('Conflict created', {
        conflictId: conflict._id,
        entityType,
        cartaId,
        floraId,
        priority,
        fieldCount: fields.length
      });

      // Attempt auto-resolution if strategy allows
      if (conflict.resolution.strategy !== 'manual') {
        const autoResolved = await this.attemptAutoResolution(conflict);
        if (autoResolved) {
          logger.info('Conflict auto-resolved', {
            conflictId: conflict._id,
            strategy: conflict.resolution.strategy
          });
        }
      }

      return conflict;

    } catch (error) {
      logger.error('Failed to create conflict', {
        entityType: conflictData?.entityType,
        error: error.message
      });
      throw error;
    }
  }

  /**
   * Detect field-level conflicts between Flora and mapped Carta data
   * @param {Object} floraData - Current Flora entity data
   * @param {Object} mappedData - Mapped Carta data
   * @param {String} entityType - Type of entity
   * @returns {Array} Array of field conflicts
   */
  detectFieldConflicts(floraData, mappedData, entityType) {
    const conflicts = [];
    const criticalFields = this.getCriticalFields(entityType);

    for (const fieldPath of criticalFields) {
      const floraValue = this.getNestedValue(floraData, fieldPath);
      const cartaValue = this.getNestedValue(mappedData, fieldPath);

      if (this.valuesAreDifferent(floraValue, cartaValue, fieldPath)) {
        conflicts.push({
          fieldName: fieldPath,
          cartaValue,
          floraValue,
          conflictReason: this.determineConflictReason(floraValue, cartaValue),
          severity: this.getFieldSeverity(fieldPath, entityType),
          dataType: typeof floraValue,
          isResolvable: this.isAutoResolvable(floraValue, cartaValue)
        });
      }
    }

    return conflicts;
  }

  /**
   * Attempt automatic conflict resolution
   * @param {Object} conflict - Conflict record
   * @returns {Promise<Boolean>} True if auto-resolved
   */
  async attemptAutoResolution(conflict) {
    try {
      const strategy = conflict.resolution?.strategy;

      if (!strategy || strategy === 'manual') {
        return false;
      }

      logger.info('Attempting auto-resolution', {
        conflictId: conflict._id,
        strategy
      });

      let resolved = false;

      switch (strategy) {
        case 'carta_wins':
          resolved = await this.resolveCartaWins(conflict);
          break;

        case 'flora_wins':
          resolved = await this.resolveFloraWins(conflict);
          break;

        case 'merge':
          resolved = await this.resolveMerge(conflict);
          break;

        case 'newest_wins':
          resolved = await this.resolveNewestWins(conflict);
          break;

        default:
          logger.warn('Unknown resolution strategy', { strategy });
          return false;
      }

      if (resolved) {
        conflict.autoResolution = {
          attempted: true,
          attemptedAt: new Date(),
          succeeded: true
        };
        await conflict.save();
      }

      return resolved;

    } catch (error) {
      logger.error('Auto-resolution failed', {
        conflictId: conflict._id,
        error: error.message
      });

      conflict.autoResolution = {
        attempted: true,
        attemptedAt: new Date(),
        succeeded: false,
        failureReason: error.message
      };
      await conflict.save();

      return false;
    }
  }

  /**
   * Resolve conflict using "Carta wins" strategy
   * @param {Object} conflict - Conflict record
   * @returns {Promise<Boolean>} Resolution success
   */
  async resolveCartaWins(conflict) {
    try {
      const { floraId, floraModel } = conflict.entity;
      const Model = this.getModel(floraModel);

      const entity = await Model.findById(floraId);
      if (!entity) {
        logger.error('Entity not found for resolution', { floraId, floraModel });
        return false;
      }

      const appliedChanges = [];

      // Apply all Carta values
      for (const field of conflict.conflictDetails.fields) {
        const previousValue = this.getNestedValue(entity, field.fieldName);
        this.setNestedValue(entity, field.fieldName, field.cartaValue);

        appliedChanges.push({
          field: field.fieldName,
          previousValue,
          newValue: field.cartaValue,
          source: 'carta'
        });
      }

      // Update sync status
      if (entity.cartaIntegration) {
        entity.cartaIntegration.lastCartaSync = new Date();
        entity.cartaIntegration.cartaSyncStatus = 'synced';
      }

      await entity.save();
      await conflict.autoResolve('carta_wins', appliedChanges);

      logger.info('Conflict resolved - Carta wins', {
        conflictId: conflict._id,
        changesApplied: appliedChanges.length
      });

      return true;

    } catch (error) {
      logger.error('Carta wins resolution failed', {
        conflictId: conflict._id,
        error: error.message
      });
      return false;
    }
  }

  /**
   * Resolve conflict using "Flora wins" strategy
   * @param {Object} conflict - Conflict record
   * @returns {Promise<Boolean>} Resolution success
   */
  async resolveFloraWins(conflict) {
    try {
      // Keep Flora values, just mark conflict as resolved
      const keptChanges = conflict.conflictDetails.fields.map(field => ({
        field: field.fieldName,
        previousValue: field.floraValue,
        newValue: field.floraValue,
        source: 'flora'
      }));

      await conflict.autoResolve('flora_wins', keptChanges);

      logger.info('Conflict resolved - Flora wins', {
        conflictId: conflict._id
      });

      return true;

    } catch (error) {
      logger.error('Flora wins resolution failed', {
        conflictId: conflict._id,
        error: error.message
      });
      return false;
    }
  }

  /**
   * Resolve conflict using "Merge" strategy
   * @param {Object} conflict - Conflict record
   * @returns {Promise<Boolean>} Resolution success
   */
  async resolveMerge(conflict) {
    try {
      const { floraId, floraModel } = conflict.entity;
      const Model = this.getModel(floraModel);

      const entity = await Model.findById(floraId);
      if (!entity) return false;

      const appliedChanges = [];

      // Apply intelligent merge logic for each field
      for (const field of conflict.conflictDetails.fields) {
        const mergedValue = this.mergeFieldValues(
          field.floraValue,
          field.cartaValue,
          field.fieldName,
          field.dataType
        );

        const previousValue = this.getNestedValue(entity, field.fieldName);
        this.setNestedValue(entity, field.fieldName, mergedValue);

        appliedChanges.push({
          field: field.fieldName,
          previousValue,
          newValue: mergedValue,
          source: 'merged',
          mergeMethod: this.getMergeMethod(field.dataType)
        });
      }

      await entity.save();
      await conflict.autoResolve('merge', appliedChanges);

      logger.info('Conflict resolved - Merge', {
        conflictId: conflict._id,
        changesApplied: appliedChanges.length
      });

      return true;

    } catch (error) {
      logger.error('Merge resolution failed', {
        conflictId: conflict._id,
        error: error.message
      });
      return false;
    }
  }

  /**
   * Resolve conflict using "Newest wins" strategy
   * @param {Object} conflict - Conflict record
   * @returns {Promise<Boolean>} Resolution success
   */
  async resolveNewestWins(conflict) {
    try {
      const cartaModified = conflict.conflictDetails.cartaLastModifiedAt;
      const floraModified = conflict.conflictDetails.floraLastModifiedAt;

      if (cartaModified > floraModified) {
        return await this.resolveCartaWins(conflict);
      } else {
        return await this.resolveFloraWins(conflict);
      }

    } catch (error) {
      logger.error('Newest wins resolution failed', {
        conflictId: conflict._id,
        error: error.message
      });
      return false;
    }
  }

  /**
   * Merge field values intelligently based on data type
   * @param {*} floraValue - Flora field value
   * @param {*} cartaValue - Carta field value
   * @param {String} fieldName - Field name
   * @param {String} dataType - Data type
   * @returns {*} Merged value
   */
  mergeFieldValues(floraValue, cartaValue, fieldName, dataType) {
    // Numbers: take the larger/more recent value
    if (dataType === 'number' && typeof floraValue === 'number' && typeof cartaValue === 'number') {
      // For monetary values and metrics, prefer Carta (source of truth)
      if (fieldName.includes('amount') || fieldName.includes('value') || fieldName.includes('commitment')) {
        return cartaValue;
      }
      return Math.max(floraValue, cartaValue);
    }

    // Dates: take the newer date
    if (floraValue instanceof Date && cartaValue instanceof Date) {
      return floraValue > cartaValue ? floraValue : cartaValue;
    }

    // Arrays: merge and deduplicate
    if (Array.isArray(floraValue) && Array.isArray(cartaValue)) {
      const merged = [...floraValue, ...cartaValue];
      return [...new Set(merged.map(item => JSON.stringify(item)))].map(item => JSON.parse(item));
    }

    // Strings: prefer non-empty, longer value
    if (typeof floraValue === 'string' && typeof cartaValue === 'string') {
      if (!cartaValue || cartaValue.length === 0) return floraValue;
      if (!floraValue || floraValue.length === 0) return cartaValue;
      return cartaValue.length > floraValue.length ? cartaValue : floraValue;
    }

    // Objects: merge properties
    if (typeof floraValue === 'object' && typeof cartaValue === 'object' && floraValue !== null && cartaValue !== null) {
      return { ...floraValue, ...cartaValue };
    }

    // Boolean: prefer true over false
    if (typeof floraValue === 'boolean' && typeof cartaValue === 'boolean') {
      return floraValue || cartaValue;
    }

    // Default: prefer Carta value (source of truth)
    return cartaValue;
  }

  /**
   * Get critical fields for conflict detection by entity type
   * @param {String} entityType - Entity type
   * @returns {Array} Array of field paths
   */
  getCriticalFields(entityType) {
    const criticalFields = {
      fund: [
        'name',
        'totalCommitment',
        'configuration.managementFee',
        'configuration.carriedInterest',
        'configuration.hurdle',
        'metrics.calledCapital',
        'metrics.paidInCapital',
        'metrics.nav'
      ],
      investment: [
        'companyName',
        'initialInvestment.amount',
        'initialInvestment.date',
        'currentValuation.fmv',
        'currentValuation.ownership',
        'performance.moic',
        'performance.irr'
      ],
      stakeholder: [
        'name',
        'email',
        'lpDetails.commitmentAmount',
        'lpDetails.calledAmount',
        'lpDetails.paidInAmount',
        'lpDetails.distributedAmount',
        'contactInfo.phone',
        'contactInfo.address'
      ],
      capTable: [
        'summary.totalOutstandingShares',
        'summary.fullyDilutedShares',
        'summary.optionPoolShares',
        'valuation.postMoney',
        'valuation.pricePerShare'
      ]
    };

    return criticalFields[entityType] || [];
  }

  /**
   * Calculate conflict priority
   * @param {Array} fields - Conflicting fields
   * @param {String} entityType - Entity type
   * @returns {String} Priority level
   */
  calculatePriority(fields, entityType) {
    if (fields.length === 0) return 'low';

    // Check for critical severity fields
    const hasCritical = fields.some(f => f.severity === 'critical');
    if (hasCritical) return 'urgent';

    // Check for high severity fields
    const hasHigh = fields.some(f => f.severity === 'high');
    if (hasHigh) return 'high';

    // Multiple medium severity fields = high priority
    const mediumCount = fields.filter(f => f.severity === 'medium').length;
    if (mediumCount >= 3) return 'high';
    if (mediumCount >= 1) return 'medium';

    return 'low';
  }

  /**
   * Get SLA target time in minutes
   * @param {String} entityType - Entity type
   * @param {String} priority - Priority level
   * @returns {Number} Target time in minutes
   */
  getSLATarget(entityType, priority) {
    const slaTargets = {
      urgent: 60,      // 1 hour
      high: 240,       // 4 hours
      medium: 1440,    // 24 hours
      low: 4320        // 3 days
    };

    return slaTargets[priority] || 1440;
  }

  /**
   * Get default resolution strategy
   * @param {String} entityType - Entity type
   * @param {String} conflictType - Conflict type
   * @param {Array} fields - Conflicting fields
   * @returns {String} Strategy name
   */
  getDefaultStrategy(entityType, conflictType, fields) {
    // For deleted entities, always manual
    if (conflictType === 'deleted_in_source' || conflictType === 'deleted_in_target') {
      return 'manual';
    }

    // For validation errors, manual review
    if (conflictType === 'validation_error' || conflictType === 'business_rule_violation') {
      return 'manual';
    }

    // If all fields are auto-resolvable, use merge
    const allResolvable = fields.every(f => f.isResolvable);
    if (allResolvable && fields.length <= 3) {
      return 'merge';
    }

    // For financial data, prefer Carta (source of truth)
    const hasFinancialData = fields.some(f =>
      f.fieldName.includes('amount') ||
      f.fieldName.includes('commitment') ||
      f.fieldName.includes('capital')
    );

    if (hasFinancialData) {
      return 'carta_wins';
    }

    // For concurrent updates, use newest
    if (conflictType === 'concurrent_update') {
      return 'newest_wins';
    }

    // Default to Carta wins (source of truth)
    return 'carta_wins';
  }

  /**
   * Get field severity
   * @param {String} fieldPath - Field path
   * @param {String} entityType - Entity type
   * @returns {String} Severity level
   */
  getFieldSeverity(fieldPath, entityType) {
    // Critical fields - financial data and legal information
    const criticalPatterns = [
      'commitment',
      'capital',
      'shares',
      'ownership',
      'valuation',
      'fmv',
      'amount',
      'ein',
      'taxId',
      'legalName'
    ];

    if (criticalPatterns.some(pattern => fieldPath.toLowerCase().includes(pattern.toLowerCase()))) {
      return 'critical';
    }

    // High severity - performance metrics and key dates
    const highPatterns = ['irr', 'moic', 'nav', 'date', 'status'];
    if (highPatterns.some(pattern => fieldPath.toLowerCase().includes(pattern.toLowerCase()))) {
      return 'high';
    }

    // Medium severity - contact information
    const mediumPatterns = ['email', 'phone', 'address', 'name'];
    if (mediumPatterns.some(pattern => fieldPath.toLowerCase().includes(pattern.toLowerCase()))) {
      return 'medium';
    }

    return 'low';
  }

  /**
   * Determine conflict type
   * @param {Object} floraData - Flora data
   * @param {Object} mappedData - Mapped Carta data
   * @param {Object} cartaData - Raw Carta data
   * @returns {String} Conflict type
   */
  determineConflictType(floraData, mappedData, cartaData) {
    // Check if entity is deleted in source
    if (cartaData.is_deleted || cartaData.status === 'deleted') {
      return 'deleted_in_source';
    }

    // Check for concurrent updates (both modified recently)
    const floraModified = floraData.updatedAt;
    const cartaModified = cartaData.updated_at ? new Date(cartaData.updated_at) : null;

    if (floraModified && cartaModified) {
      const timeDiff = Math.abs(floraModified - cartaModified);
      if (timeDiff < 60000) { // Within 1 minute
        return 'concurrent_update';
      }
    }

    return 'data_mismatch';
  }

  /**
   * Determine conflict reason
   * @param {*} floraValue - Flora value
   * @param {*} cartaValue - Carta value
   * @returns {String} Conflict reason
   */
  determineConflictReason(floraValue, cartaValue) {
    if (floraValue === null || floraValue === undefined) {
      return 'Missing in Flora';
    }
    if (cartaValue === null || cartaValue === undefined) {
      return 'Missing in Carta';
    }
    if (typeof floraValue !== typeof cartaValue) {
      return 'Data type mismatch';
    }
    return 'Values differ';
  }

  /**
   * Check if conflict is auto-resolvable
   * @param {*} floraValue - Flora value
   * @param {*} cartaValue - Carta value
   * @returns {Boolean} Is auto-resolvable
   */
  isAutoResolvable(floraValue, cartaValue) {
    // Null/undefined conflicts are resolvable
    if (!floraValue || !cartaValue) return true;

    // Simple type conflicts are resolvable
    const simpleTypes = ['string', 'number', 'boolean'];
    if (simpleTypes.includes(typeof floraValue) && simpleTypes.includes(typeof cartaValue)) {
      return true;
    }

    // Date conflicts are resolvable
    if (floraValue instanceof Date && cartaValue instanceof Date) {
      return true;
    }

    // Complex objects require manual review
    return false;
  }

  /**
   * Check if values are different
   * @param {*} floraValue - Flora value
   * @param {*} cartaValue - Carta value
   * @param {String} fieldPath - Field path
   * @returns {Boolean} Are values different
   */
  valuesAreDifferent(floraValue, cartaValue, fieldPath) {
    // Handle null/undefined
    if (floraValue == null && cartaValue == null) return false;
    if (floraValue == null || cartaValue == null) return true;

    // Handle dates
    if (floraValue instanceof Date && cartaValue instanceof Date) {
      return Math.abs(floraValue - cartaValue) > 1000; // 1 second tolerance
    }

    // Handle numbers with tolerance for floating point
    if (typeof floraValue === 'number' && typeof cartaValue === 'number') {
      const tolerance = 0.01;
      return Math.abs(floraValue - cartaValue) > tolerance;
    }

    // Handle arrays
    if (Array.isArray(floraValue) && Array.isArray(cartaValue)) {
      if (floraValue.length !== cartaValue.length) return true;
      return JSON.stringify(floraValue.sort()) !== JSON.stringify(cartaValue.sort());
    }

    // Handle objects
    if (typeof floraValue === 'object' && typeof cartaValue === 'object') {
      return JSON.stringify(floraValue) !== JSON.stringify(cartaValue);
    }

    // Default comparison
    return floraValue !== cartaValue;
  }

  /**
   * Get model by name
   * @param {String} modelName - Model name
   * @returns {Object} Mongoose model
   */
  getModel(modelName) {
    const models = {
      'Fund': require('../../models/Fund'),
      'Investment': require('../../models/Investment'),
      'Stakeholder': require('../../models/Stakeholder'),
      'CapTable': require('../../models/CapTable')
    };

    return models[modelName];
  }

  /**
   * Get model name from entity type
   * @param {String} entityType - Entity type
   * @returns {String} Model name
   */
  getModelName(entityType) {
    const modelMap = {
      'fund': 'Fund',
      'investment': 'Investment',
      'stakeholder': 'Stakeholder',
      'capTable': 'CapTable'
    };

    return modelMap[entityType];
  }

  /**
   * Get entity description
   * @param {Object} floraData - Flora entity data
   * @param {String} entityType - Entity type
   * @returns {String} Entity description
   */
  getEntityDescription(floraData, entityType) {
    switch (entityType) {
      case 'fund':
        return `${floraData.name} (${floraData.vintage?.getFullYear() || 'N/A'})`;
      case 'investment':
        return `${floraData.companyName} - ${floraData.initialInvestment?.round || 'N/A'}`;
      case 'stakeholder':
        return `${floraData.name} (${floraData.investorType || 'N/A'})`;
      case 'capTable':
        return `Cap Table Snapshot ${floraData.snapshotDate?.toISOString().split('T')[0] || 'N/A'}`;
      default:
        return 'Unknown';
    }
  }

  /**
   * Generate conflict summary
   * @param {Array} fields - Conflicting fields
   * @returns {String} Summary text
   */
  generateConflictSummary(fields) {
    if (fields.length === 0) return 'No conflicts detected';

    const criticalCount = fields.filter(f => f.severity === 'critical').length;
    const highCount = fields.filter(f => f.severity === 'high').length;

    let summary = `${fields.length} field conflict${fields.length > 1 ? 's' : ''} detected`;

    if (criticalCount > 0) {
      summary += ` (${criticalCount} critical)`;
    } else if (highCount > 0) {
      summary += ` (${highCount} high severity)`;
    }

    return summary;
  }

  /**
   * Get merge method description
   * @param {String} dataType - Data type
   * @returns {String} Merge method
   */
  getMergeMethod(dataType) {
    const methods = {
      'number': 'Maximum value',
      'string': 'Longest non-empty value',
      'boolean': 'Logical OR',
      'object': 'Deep merge',
      'undefined': 'Prefer Carta value'
    };

    return methods[dataType] || 'Default merge';
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
   * Set nested value in object
   * @param {Object} obj - Object
   * @param {String} path - Dot-separated path
   * @param {*} value - Value to set
   */
  setNestedValue(obj, path, value) {
    const keys = path.split('.');
    const lastKey = keys.pop();
    const target = keys.reduce((current, key) => {
      if (!(key in current)) {
        current[key] = {};
      }
      return current[key];
    }, obj);
    target[lastKey] = value;
  }
}

module.exports = new CartaConflictResolver();
