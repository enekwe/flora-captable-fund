/**
 * Carta Data Mapper Service
 * Handles bidirectional data transformation between Carta and Flora schemas
 * Includes field-level mapping, validation, and custom field support
 */

const CartaFieldMapping = require('../../models/carta/CartaFieldMapping');
const logger = require('../../utils/logger');

class CartaDataMapper {
  /**
   * Map Carta stakeholder to Flora stakeholder
   * @param {Object} cartaStakeholder - Stakeholder data from Carta
   * @param {Object} fieldMappings - Custom field mappings (optional)
   * @returns {Promise<Object>} Mapped Flora stakeholder data
   */
  async mapStakeholder(cartaStakeholder, fieldMappings = null) {
    try {
      const mapped = {
        // Basic Information
        name: cartaStakeholder.name || cartaStakeholder.legal_name,
        legalName: cartaStakeholder.legal_name,
        email: cartaStakeholder.email,

        // Contact Information
        contactInfo: {
          phone: cartaStakeholder.phone,
          address: this.mapAddress(cartaStakeholder.address),
          secondaryEmail: cartaStakeholder.secondary_email
        },

        // Investor Type Classification
        investorType: this.mapInvestorType(cartaStakeholder.type),
        entityType: this.mapEntityType(cartaStakeholder.entity_type),

        // Tax Information
        taxInfo: {
          taxId: cartaStakeholder.tax_id,
          taxIdType: cartaStakeholder.tax_id_type,
          w9OnFile: cartaStakeholder.w9_on_file || false,
          foreignTaxId: cartaStakeholder.foreign_tax_id
        }
      };

      // LP-specific details
      if (cartaStakeholder.type === 'LP' || cartaStakeholder.type === 'investor') {
        mapped.lpDetails = {
          commitmentAmount: cartaStakeholder.commitment_amount,
          calledAmount: cartaStakeholder.called_capital,
          paidInAmount: cartaStakeholder.paid_in_capital,
          distributedAmount: cartaStakeholder.distributions_total,
          unfundedCommitment: cartaStakeholder.unfunded_commitment,
          netAssetValue: cartaStakeholder.nav,
          managementFeeRate: cartaStakeholder.management_fee_rate,
          carriedInterestRate: cartaStakeholder.carried_interest_rate,
          lpClass: cartaStakeholder.lp_class,
          isAccredited: cartaStakeholder.is_accredited,
          isQualified: cartaStakeholder.is_qualified_purchaser,
          commitmentDate: cartaStakeholder.commitment_date ? new Date(cartaStakeholder.commitment_date) : null
        };
      }

      // GP-specific details
      if (cartaStakeholder.type === 'GP' || cartaStakeholder.type === 'general_partner') {
        mapped.gpDetails = {
          carriedInterest: cartaStakeholder.carried_interest_percentage,
          managementCompanyName: cartaStakeholder.management_company,
          isKeyPerson: cartaStakeholder.is_key_person || false
        };
      }

      // Individual vs Entity specific fields
      if (cartaStakeholder.entity_type === 'individual') {
        mapped.individualInfo = {
          firstName: cartaStakeholder.first_name,
          lastName: cartaStakeholder.last_name,
          middleName: cartaStakeholder.middle_name,
          dateOfBirth: cartaStakeholder.date_of_birth ? new Date(cartaStakeholder.date_of_birth) : null,
          citizenship: cartaStakeholder.citizenship,
          ssn: cartaStakeholder.ssn
        };
      } else {
        mapped.entityInfo = {
          entityName: cartaStakeholder.entity_name || cartaStakeholder.name,
          ein: cartaStakeholder.ein,
          stateOfFormation: cartaStakeholder.state_of_formation,
          dateOfFormation: cartaStakeholder.date_of_formation ? new Date(cartaStakeholder.date_of_formation) : null,
          entityStructure: cartaStakeholder.entity_structure
        };
      }

      // Compliance and Documentation
      mapped.compliance = {
        kycCompleted: cartaStakeholder.kyc_completed || false,
        amlCheckCompleted: cartaStakeholder.aml_check_completed || false,
        accreditationVerified: cartaStakeholder.accreditation_verified || false,
        accreditationExpiryDate: cartaStakeholder.accreditation_expiry ? new Date(cartaStakeholder.accreditation_expiry) : null
      };

      // Apply custom field mappings if provided
      if (fieldMappings) {
        await this.applyCustomMappings(mapped, cartaStakeholder, fieldMappings, 'stakeholder');
      }

      return mapped;

    } catch (error) {
      logger.error('Failed to map Carta stakeholder', {
        cartaId: cartaStakeholder?.id,
        error: error.message
      });
      throw error;
    }
  }

  /**
   * Map Carta fund to Flora fund
   * @param {Object} cartaFund - Fund data from Carta
   * @returns {Promise<Object>} Mapped Flora fund data
   */
  async mapFund(cartaFund) {
    try {
      const mapped = {
        // Basic Information
        name: cartaFund.name,
        legalName: cartaFund.legal_name,
        description: cartaFund.description,

        // Fund Structure
        fundType: this.mapFundType(cartaFund.fund_type),
        vintage: cartaFund.vintage_year ? new Date(cartaFund.vintage_year, 0, 1) : null,
        status: this.mapFundStatus(cartaFund.status),

        // Financial Details
        totalCommitment: cartaFund.total_commitment,
        targetSize: cartaFund.target_size,
        hardCap: cartaFund.hard_cap,
        currency: cartaFund.currency || 'USD',

        // Configuration
        configuration: {
          managementFee: cartaFund.management_fee_rate,
          carriedInterest: cartaFund.carried_interest_rate,
          hurdle: cartaFund.hurdle_rate,
          preferredReturn: cartaFund.preferred_return,
          waterfall: this.mapWaterfallType(cartaFund.waterfall_type),
          catchUp: cartaFund.catch_up_rate,
          term: cartaFund.fund_term_years,
          investmentPeriod: cartaFund.investment_period_years,
          extensionOptions: cartaFund.extension_options
        },

        // Dates
        dates: {
          firstClose: cartaFund.first_close_date ? new Date(cartaFund.first_close_date) : null,
          finalClose: cartaFund.final_close_date ? new Date(cartaFund.final_close_date) : null,
          inception: cartaFund.inception_date ? new Date(cartaFund.inception_date) : null,
          terminationDate: cartaFund.termination_date ? new Date(cartaFund.termination_date) : null
        },

        // Investment Strategy
        strategy: {
          focus: cartaFund.investment_focus,
          geography: cartaFund.geographic_focus,
          sectors: cartaFund.sector_focus || [],
          stages: cartaFund.investment_stages || [],
          checkSizeMin: cartaFund.min_check_size,
          checkSizeMax: cartaFund.max_check_size
        },

        // Performance Metrics
        metrics: {
          calledCapital: cartaFund.called_capital,
          paidInCapital: cartaFund.paid_in_capital,
          distributedCapital: cartaFund.distributed_capital,
          remainingCommitment: cartaFund.remaining_commitment,
          nav: cartaFund.nav,
          tvpi: cartaFund.tvpi,
          dpi: cartaFund.dpi,
          rvpi: cartaFund.rvpi,
          irr: cartaFund.irr,
          moic: cartaFund.moic
        },

        // Legal and Regulatory
        legal: {
          jurisdiction: cartaFund.jurisdiction,
          structureType: cartaFund.structure_type,
          registeredName: cartaFund.registered_name,
          registrationNumber: cartaFund.registration_number,
          isSEC_registered: cartaFund.is_sec_registered || false
        }
      };

      return mapped;

    } catch (error) {
      logger.error('Failed to map Carta fund', {
        cartaId: cartaFund?.id,
        error: error.message
      });
      throw error;
    }
  }

  /**
   * Map Carta investment to Flora investment
   * @param {Object} cartaInvestment - Investment data from Carta
   * @returns {Promise<Object>} Mapped Flora investment data
   */
  async mapInvestment(cartaInvestment) {
    try {
      const mapped = {
        // Company Information
        companyName: cartaInvestment.company_name,
        legalName: cartaInvestment.company_legal_name,
        description: cartaInvestment.company_description,
        website: cartaInvestment.company_website,

        // Investment Details
        initialInvestment: {
          amount: cartaInvestment.initial_investment_amount,
          date: cartaInvestment.investment_date ? new Date(cartaInvestment.investment_date) : null,
          round: cartaInvestment.round_name,
          series: cartaInvestment.series,
          preMoney: cartaInvestment.pre_money_valuation,
          postMoney: cartaInvestment.post_money_valuation,
          sharePrice: cartaInvestment.share_price,
          sharesAcquired: cartaInvestment.shares_acquired,
          ownership: cartaInvestment.ownership_percentage ? cartaInvestment.ownership_percentage / 100 : null
        },

        // Current Status
        currentValuation: {
          fmv: cartaInvestment.current_valuation,
          lastUpdated: cartaInvestment.valuation_date ? new Date(cartaInvestment.valuation_date) : null,
          ownership: cartaInvestment.current_ownership_percentage ? cartaInvestment.current_ownership_percentage / 100 : null,
          sharesOwned: cartaInvestment.current_shares_owned,
          fullyDilutedOwnership: cartaInvestment.fully_diluted_ownership ? cartaInvestment.fully_diluted_ownership / 100 : null
        },

        // Follow-on Investments
        followOnInvestments: (cartaInvestment.follow_on_rounds || []).map(round => ({
          amount: round.amount,
          date: round.date ? new Date(round.date) : null,
          round: round.round_name,
          preMoney: round.pre_money_valuation,
          postMoney: round.post_money_valuation,
          sharesAcquired: round.shares_acquired
        })),

        // Security Details
        securities: (cartaInvestment.securities || []).map(sec => ({
          type: sec.security_type,
          quantity: sec.quantity,
          securityId: sec.security_id,
          certificateNumber: sec.certificate_number,
          issueDate: sec.issue_date ? new Date(sec.issue_date) : null
        })),

        // Company Details
        companyDetails: {
          industry: cartaInvestment.industry,
          sector: cartaInvestment.sector,
          stage: cartaInvestment.company_stage,
          foundedDate: cartaInvestment.founded_date ? new Date(cartaInvestment.founded_date) : null,
          headquarters: cartaInvestment.headquarters_location,
          employeeCount: cartaInvestment.employee_count,
          revenue: cartaInvestment.annual_revenue
        },

        // Rights and Terms
        investorRights: {
          boardSeat: cartaInvestment.has_board_seat || false,
          boardObserver: cartaInvestment.has_board_observer_rights || false,
          proRataRights: cartaInvestment.has_pro_rata_rights || false,
          informationRights: cartaInvestment.has_information_rights || false,
          dragAlongRights: cartaInvestment.has_drag_along || false,
          tagAlongRights: cartaInvestment.has_tag_along || false,
          antiDilution: cartaInvestment.anti_dilution_protection
        },

        // Performance Metrics
        performance: {
          unrealizedValue: cartaInvestment.unrealized_value,
          realizedValue: cartaInvestment.realized_value,
          totalValue: cartaInvestment.total_value,
          moic: cartaInvestment.moic,
          irr: cartaInvestment.irr,
          distributionsReceived: cartaInvestment.distributions_received,
          fairMarketValue: cartaInvestment.fmv
        },

        // Status
        status: this.mapInvestmentStatus(cartaInvestment.status),
        exitDate: cartaInvestment.exit_date ? new Date(cartaInvestment.exit_date) : null,
        exitType: cartaInvestment.exit_type,
        exitValuation: cartaInvestment.exit_valuation
      };

      return mapped;

    } catch (error) {
      logger.error('Failed to map Carta investment', {
        cartaId: cartaInvestment?.id,
        error: error.message
      });
      throw error;
    }
  }

  /**
   * Map Carta cap table to Flora cap table
   * @param {Object} cartaCapTable - Cap table data from Carta
   * @returns {Promise<Object>} Mapped Flora cap table data
   */
  async mapCapTable(cartaCapTable) {
    try {
      const mapped = {
        // Snapshot Information
        snapshotDate: cartaCapTable.snapshot_date ? new Date(cartaCapTable.snapshot_date) : new Date(),
        snapshotType: cartaCapTable.snapshot_type || 'current',

        // Summary Statistics
        summary: {
          totalAuthorizedShares: cartaCapTable.authorized_shares,
          totalIssuedShares: cartaCapTable.issued_shares,
          totalOutstandingShares: cartaCapTable.outstanding_shares,
          fullyDilutedShares: cartaCapTable.fully_diluted_shares,
          optionPoolShares: cartaCapTable.option_pool_shares,
          availableOptionPoolShares: cartaCapTable.available_option_pool_shares,
          reservedShares: cartaCapTable.reserved_shares,
          treasuryShares: cartaCapTable.treasury_shares
        },

        // Ownership Distribution
        ownership: (cartaCapTable.ownership || []).map(owner => ({
          stakeholderId: owner.stakeholder_id,
          stakeholderName: owner.stakeholder_name,
          shares: owner.shares,
          percentage: owner.percentage ? owner.percentage / 100 : 0,
          fullyDilutedPercentage: owner.fully_diluted_percentage ? owner.fully_diluted_percentage / 100 : 0,
          securityType: owner.security_type,
          stockClass: owner.stock_class,
          isFounder: owner.is_founder || false,
          isEmployee: owner.is_employee || false,
          vestingSchedule: owner.vesting_schedule
        })),

        // Stock Classes
        stockClasses: (cartaCapTable.stock_classes || []).map(sc => ({
          name: sc.name,
          className: sc.class_name,
          authorizedShares: sc.authorized_shares,
          issuedShares: sc.issued_shares,
          outstandingShares: sc.outstanding_shares,
          parValue: sc.par_value,
          votingRights: sc.voting_rights,
          liquidationPreference: sc.liquidation_preference,
          liquidationMultiple: sc.liquidation_multiple,
          dividendRate: sc.dividend_rate,
          isParticipating: sc.is_participating || false,
          conversionRatio: sc.conversion_ratio,
          conversionPrice: sc.conversion_price
        })),

        // Convertible Securities
        convertibles: (cartaCapTable.convertible_securities || []).map(conv => ({
          type: conv.type,
          principal: conv.principal_amount,
          interestRate: conv.interest_rate,
          discountRate: conv.discount_rate,
          valuationCap: conv.valuation_cap,
          maturityDate: conv.maturity_date ? new Date(conv.maturity_date) : null,
          issueDate: conv.issue_date ? new Date(conv.issue_date) : null,
          holder: conv.holder_name
        })),

        // Option Pool
        optionPool: {
          totalShares: cartaCapTable.option_pool_total,
          grantedShares: cartaCapTable.option_pool_granted,
          vestedShares: cartaCapTable.option_pool_vested,
          exercisedShares: cartaCapTable.option_pool_exercised,
          availableShares: cartaCapTable.option_pool_available,
          poolPercentage: cartaCapTable.option_pool_percentage ? cartaCapTable.option_pool_percentage / 100 : null
        },

        // Valuation
        valuation: {
          preMoney: cartaCapTable.pre_money_valuation,
          postMoney: cartaCapTable.post_money_valuation,
          pricePerShare: cartaCapTable.price_per_share,
          valuationDate: cartaCapTable.valuation_date ? new Date(cartaCapTable.valuation_date) : null,
          valuationMethod: cartaCapTable.valuation_method,
          source: cartaCapTable.valuation_source
        },

        // Round Information
        fundingRounds: (cartaCapTable.funding_rounds || []).map(round => ({
          name: round.name,
          series: round.series,
          closeDate: round.close_date ? new Date(round.close_date) : null,
          amountRaised: round.amount_raised,
          preMoney: round.pre_money_valuation,
          postMoney: round.post_money_valuation,
          investors: round.investors || []
        }))
      };

      return mapped;

    } catch (error) {
      logger.error('Failed to map Carta cap table', {
        cartaId: cartaCapTable?.id,
        error: error.message
      });
      throw error;
    }
  }

  /**
   * Map Carta valuation to Flora valuation
   * @param {Object} cartaValuation - Valuation data from Carta
   * @returns {Promise<Object>} Mapped Flora valuation data
   */
  async mapValuation(cartaValuation) {
    try {
      return {
        fmv: cartaValuation.fair_market_value,
        commonStockPrice: cartaValuation.common_stock_price,
        preferredStockPrice: cartaValuation.preferred_stock_price,
        valuationDate: cartaValuation.valuation_date ? new Date(cartaValuation.valuation_date) : null,
        source: this.mapValuationSource(cartaValuation.source),
        methodology: this.mapValuationMethodology(cartaValuation.methodology),
        preMoney: cartaValuation.pre_money_valuation,
        postMoney: cartaValuation.post_money_valuation,
        enterpriseValue: cartaValuation.enterprise_value,
        equityValue: cartaValuation.equity_value,
        is409A: cartaValuation.is_409a || false,
        validUntil: cartaValuation.valid_until ? new Date(cartaValuation.valid_until) : null
      };
    } catch (error) {
      logger.error('Failed to map Carta valuation', {
        cartaId: cartaValuation?.id,
        error: error.message
      });
      throw error;
    }
  }

  // ============ Reverse Mapping (Flora to Carta) ============

  /**
   * Map Flora stakeholder to Carta stakeholder format
   * @param {Object} floraStakeholder - Flora stakeholder data
   * @returns {Object} Carta-formatted stakeholder data
   */
  mapFloraToCartaStakeholder(floraStakeholder) {
    const cartaData = {
      name: floraStakeholder.name,
      legal_name: floraStakeholder.legalName || floraStakeholder.name,
      email: floraStakeholder.email,
      phone: floraStakeholder.contactInfo?.phone,
      type: this.reverseMapInvestorType(floraStakeholder.investorType),
      entity_type: this.reverseMapEntityType(floraStakeholder.entityType)
    };

    // Add address if present
    if (floraStakeholder.contactInfo?.address) {
      cartaData.address = this.reverseMapAddress(floraStakeholder.contactInfo.address);
    }

    // Add LP details if present
    if (floraStakeholder.lpDetails) {
      cartaData.commitment_amount = floraStakeholder.lpDetails.commitmentAmount;
      cartaData.called_capital = floraStakeholder.lpDetails.calledAmount;
      cartaData.paid_in_capital = floraStakeholder.lpDetails.paidInAmount;
    }

    // Add tax information
    if (floraStakeholder.taxInfo) {
      cartaData.tax_id = floraStakeholder.taxInfo.taxId;
      cartaData.tax_id_type = floraStakeholder.taxInfo.taxIdType;
    }

    return cartaData;
  }

  /**
   * Map Flora fund to Carta fund format
   * @param {Object} floraFund - Flora fund data
   * @returns {Object} Carta-formatted fund data
   */
  mapFloraToCartaFund(floraFund) {
    return {
      name: floraFund.name,
      legal_name: floraFund.legalName,
      total_commitment: floraFund.totalCommitment,
      vintage_year: floraFund.vintage ? floraFund.vintage.getFullYear() : null,
      status: this.reverseMapFundStatus(floraFund.status),
      management_fee_rate: floraFund.configuration?.managementFee,
      carried_interest_rate: floraFund.configuration?.carriedInterest,
      hurdle_rate: floraFund.configuration?.hurdle,
      fund_term_years: floraFund.configuration?.term,
      currency: floraFund.currency || 'USD'
    };
  }

  // ============ Helper Mapping Functions ============

  /**
   * Map address object
   */
  mapAddress(cartaAddress) {
    if (!cartaAddress) return null;

    return {
      street1: cartaAddress.street1 || cartaAddress.address_line_1,
      street2: cartaAddress.street2 || cartaAddress.address_line_2,
      city: cartaAddress.city,
      state: cartaAddress.state || cartaAddress.province,
      postalCode: cartaAddress.postal_code || cartaAddress.zip,
      country: cartaAddress.country,
      isPrimary: cartaAddress.is_primary || true
    };
  }

  /**
   * Reverse map address object
   */
  reverseMapAddress(floraAddress) {
    if (!floraAddress) return null;

    return {
      address_line_1: floraAddress.street1,
      address_line_2: floraAddress.street2,
      city: floraAddress.city,
      state: floraAddress.state,
      zip: floraAddress.postalCode,
      country: floraAddress.country
    };
  }

  /**
   * Map investor type from Carta to Flora
   */
  mapInvestorType(cartaType) {
    const typeMap = {
      'individual': 'LP',
      'institutional': 'LP',
      'investor': 'LP',
      'LP': 'LP',
      'general_partner': 'GP',
      'GP': 'GP',
      'advisor': 'advisor',
      'employee': 'employee',
      'service_provider': 'service_provider'
    };

    return typeMap[cartaType] || 'other';
  }

  /**
   * Reverse map investor type from Flora to Carta
   */
  reverseMapInvestorType(floraType) {
    const reverseMap = {
      'LP': 'investor',
      'GP': 'general_partner',
      'advisor': 'advisor',
      'employee': 'employee',
      'other': 'individual'
    };

    return reverseMap[floraType] || 'individual';
  }

  /**
   * Map entity type
   */
  mapEntityType(cartaEntityType) {
    const typeMap = {
      'individual': 'individual',
      'corporation': 'corporation',
      'llc': 'llc',
      'partnership': 'partnership',
      'trust': 'trust',
      'foundation': 'foundation'
    };

    return typeMap[cartaEntityType] || 'individual';
  }

  /**
   * Reverse map entity type
   */
  reverseMapEntityType(floraEntityType) {
    // Same mapping works both ways
    return floraEntityType;
  }

  /**
   * Map fund status
   */
  mapFundStatus(cartaStatus) {
    const statusMap = {
      'formation': 'formation',
      'fundraising': 'fundraising',
      'investing': 'investing',
      'active': 'investing',
      'harvesting': 'harvesting',
      'liquidating': 'liquidating',
      'liquidated': 'liquidated',
      'closed': 'liquidated'
    };

    return statusMap[cartaStatus] || 'formation';
  }

  /**
   * Reverse map fund status
   */
  reverseMapFundStatus(floraStatus) {
    const reverseMap = {
      'formation': 'formation',
      'fundraising': 'fundraising',
      'investing': 'active',
      'harvesting': 'harvesting',
      'liquidating': 'liquidating',
      'liquidated': 'closed'
    };

    return reverseMap[floraStatus] || 'formation';
  }

  /**
   * Map fund type
   */
  mapFundType(cartaFundType) {
    const typeMap = {
      'venture_capital': 'vc',
      'private_equity': 'pe',
      'hedge_fund': 'hedge',
      'real_estate': 'real_estate',
      'credit': 'credit',
      'other': 'other'
    };

    return typeMap[cartaFundType] || 'other';
  }

  /**
   * Map waterfall type
   */
  mapWaterfallType(cartaWaterfall) {
    const waterfallMap = {
      'american': 'american',
      'european': 'european',
      'hybrid': 'hybrid',
      'deal_by_deal': 'deal_by_deal'
    };

    return waterfallMap[cartaWaterfall] || 'american';
  }

  /**
   * Map investment status
   */
  mapInvestmentStatus(cartaStatus) {
    const statusMap = {
      'active': 'active',
      'exited': 'exited',
      'written_off': 'written_off',
      'pending': 'pending',
      'closed': 'exited'
    };

    return statusMap[cartaStatus] || 'active';
  }

  /**
   * Map valuation source
   */
  mapValuationSource(cartaSource) {
    const sourceMap = {
      '409a': '409a',
      'external_valuation': 'external',
      'financing_round': 'financing',
      'internal': 'internal',
      'market': 'market'
    };

    return sourceMap[cartaSource] || 'internal';
  }

  /**
   * Map valuation methodology
   */
  mapValuationMethodology(cartaMethodology) {
    const methodMap = {
      'market_approach': 'market',
      'income_approach': 'income',
      'asset_approach': 'asset',
      'option_pricing': 'option_pricing',
      'pwerm': 'pwerm',
      'backsolve': 'backsolve'
    };

    return methodMap[cartaMethodology] || 'market';
  }

  /**
   * Apply custom field mappings
   */
  async applyCustomMappings(mapped, cartaData, fieldMappings, entityType) {
    try {
      for (const mapping of fieldMappings) {
        const cartaValue = this.getNestedValue(cartaData, mapping.cartaField.name);

        if (cartaValue !== undefined && cartaValue !== null) {
          const transformedValue = await this.transformValue(
            cartaValue,
            mapping.transformation,
            mapping.cartaField.dataType,
            mapping.floraField.dataType
          );

          this.setNestedValue(mapped, mapping.floraField.name, transformedValue);
        }
      }
    } catch (error) {
      logger.error('Failed to apply custom mappings', {
        entityType,
        error: error.message
      });
    }
  }

  /**
   * Transform value based on transformation type
   */
  async transformValue(value, transformation, sourceType, targetType) {
    switch (transformation.type) {
      case 'direct':
        return this.convertDataType(value, sourceType, targetType);

      case 'format':
        return this.applyFormat(value, transformation.format);

      case 'calculate':
        return this.calculate(value, transformation.formula);

      case 'conditional':
        return this.applyConditional(value, transformation.conditions);

      case 'lookup':
        return this.applyLookup(value, transformation.lookupTable);

      default:
        return value;
    }
  }

  /**
   * Convert data type
   */
  convertDataType(value, sourceType, targetType) {
    if (sourceType === targetType) return value;

    try {
      switch (targetType) {
        case 'number':
          return Number(value);
        case 'string':
          return String(value);
        case 'date':
          return new Date(value);
        case 'boolean':
          return Boolean(value);
        default:
          return value;
      }
    } catch (error) {
      logger.warn('Data type conversion failed', {
        value,
        sourceType,
        targetType,
        error: error.message
      });
      return value;
    }
  }

  /**
   * Apply format transformation
   */
  applyFormat(value, format) {
    // Implementation depends on format type
    // Examples: date formatting, number formatting, string formatting
    return value;
  }

  /**
   * Apply calculation
   */
  calculate(value, formula) {
    // Implementation depends on formula complexity
    // Could use a safe expression evaluator
    return value;
  }

  /**
   * Apply conditional transformation
   */
  applyConditional(value, conditions) {
    for (const condition of conditions) {
      if (this.evaluateCondition(value, condition.condition)) {
        return condition.value;
      }
    }
    return value;
  }

  /**
   * Apply lookup transformation
   */
  applyLookup(value, lookupTable) {
    return lookupTable[value] || value;
  }

  /**
   * Evaluate condition
   */
  evaluateCondition(value, condition) {
    // Simple condition evaluation
    // Could be extended for complex conditions
    return true;
  }

  /**
   * Get nested value from object using dot notation
   */
  getNestedValue(obj, path) {
    return path.split('.').reduce((current, key) => current?.[key], obj);
  }

  /**
   * Set nested value in object using dot notation
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

  /**
   * Validate mapped data
   */
  async validateMappedData(mappedData, entityType) {
    const errors = [];

    // Add validation rules based on entity type
    switch (entityType) {
      case 'stakeholder':
        if (!mappedData.name) errors.push('Name is required');
        if (!mappedData.email) errors.push('Email is required');
        break;

      case 'fund':
        if (!mappedData.name) errors.push('Fund name is required');
        if (!mappedData.totalCommitment) errors.push('Total commitment is required');
        break;

      case 'investment':
        if (!mappedData.companyName) errors.push('Company name is required');
        if (!mappedData.initialInvestment?.amount) errors.push('Investment amount is required');
        break;
    }

    return {
      isValid: errors.length === 0,
      errors
    };
  }
}

module.exports = new CartaDataMapper();
