require('dotenv').config();

const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const mongoose = require('mongoose');
const winston = require('winston');
const Bull = require('bull');
const redis = require('redis');

// Validate required environment variables
const validateEnvironment = () => {
  const required = ['ENCRYPTION_KEY', 'JWT_SECRET', 'SESSION_SECRET'];
  const missing = required.filter(key => !process.env[key]);

  if (missing.length > 0 && process.env.NODE_ENV === 'production') {
    console.error(`FATAL: Missing required environment variables: ${missing.join(', ')}`);
    console.error('Please set these variables before starting the service.');
    process.exit(1);
  } else if (missing.length > 0) {
    console.warn(`WARNING: Missing environment variables: ${missing.join(', ')}`);
    console.warn('Using development defaults - NOT SAFE FOR PRODUCTION');
  }
};

// Validate environment on startup
validateEnvironment();

// Logger setup
const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || 'info',
  format: winston.format.json(),
  transports: [
    new winston.transports.Console({
      format: winston.format.simple()
    })
  ]
});

// Create Express app
const app = express();
const PORT = process.env.PORT || 3003;

// Middleware
app.use(helmet());

// Configure CORS properly
const corsOrigins = process.env.CORS_ORIGIN
  ? process.env.CORS_ORIGIN.split(',').map(origin => origin.trim())
  : process.env.NODE_ENV === 'production'
    ? false // Deny all in production if not configured
    : ['http://localhost:3000', 'http://localhost:5173']; // Development defaults

app.use(cors({
  origin: corsOrigins,
  credentials: true,
  maxAge: 86400
}));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({
    status: 'healthy',
    service: 'captable-fund',
    timestamp: new Date().toISOString(),
    uptime: process.uptime()
  });
});

// Root endpoint
app.get('/', (req, res) => {
  res.json({
    service: 'Flora Cap Table & Fund Management',
    version: '1.0.0',
    status: 'running',
    integrations: ['carta', 'angellist', 'pulley']
  });
});

// Cap table API endpoints
app.get('/api/v1/cap-table/:fundId', (req, res) => {
  res.json({
    fundId: req.params.fundId,
    stakeholders: [],
    securities: [],
    ownership: {},
    lastUpdated: new Date().toISOString()
  });
});

app.get('/api/v1/funds', (req, res) => {
  res.json({
    funds: [],
    total: 0,
    page: 1,
    limit: 20
  });
});

app.get('/api/v1/investments', (req, res) => {
  res.json({
    investments: [],
    total: 0,
    page: 1,
    limit: 20
  });
});

// Carta integration endpoints
app.post('/api/v1/integrations/carta/connect', (req, res) => {
  logger.info('Carta connection requested');
  const authUrl = `https://auth.carta.com/oauth/authorize?client_id=${process.env.CARTA_CLIENT_ID}&redirect_uri=${process.env.CARTA_REDIRECT_URI}&response_type=code`;
  res.json({
    status: 'success',
    message: 'Carta connection initiated',
    authUrl
  });
});

app.get('/api/v1/integrations/carta/callback', (req, res) => {
  logger.info('Carta OAuth callback received');
  res.json({
    status: 'success',
    code: req.query.code,
    message: 'Authorization code received'
  });
});

app.post('/api/v1/integrations/carta/sync', async (req, res) => {
  try {
    logger.info('Carta sync requested');

    // Initialize sync queue if Redis is available
    if (process.env.REDIS_URL) {
      try {
        const syncQueue = new Bull('carta-sync', process.env.REDIS_URL);
        await syncQueue.add('sync', {
          fundId: req.body.fundId,
          timestamp: new Date().toISOString()
        });
        logger.info('Sync job added to queue');
      } catch (error) {
        logger.warn('Could not add to queue:', error.message);
      }
    }

    res.json({
      status: 'success',
      message: 'Carta sync initiated',
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    logger.error('Carta sync error:', error);
    res.status(500).json({ error: 'Sync failed' });
  }
});

app.post('/api/v1/integrations/carta/webhook', (req, res) => {
  logger.info('Carta webhook received:', req.body);
  res.json({ received: true });
});

// Database connections
const connectDatabase = async () => {
  if (process.env.MONGODB_URI) {
    try {
      await mongoose.connect(process.env.MONGODB_URI, {
        useNewUrlParser: true,
        useUnifiedTopology: true
      });
      logger.info('MongoDB connected successfully');
    } catch (error) {
      logger.warn('MongoDB connection failed:', error.message);
    }
  } else {
    logger.info('No MongoDB URI provided, running without database');
  }

  // Redis connection for Bull queues
  if (process.env.REDIS_URL) {
    try {
      const redisClient = redis.createClient({
        url: process.env.REDIS_URL
      });
      redisClient.on('error', (err) => logger.warn('Redis error:', err.message));
      await redisClient.connect();
      logger.info('Redis connected successfully for job queues');
    } catch (error) {
      logger.warn('Redis connection failed:', error.message);
    }
  } else {
    logger.info('No Redis URL provided, running without job queues');
  }
};

// Start server
const startServer = async () => {
  await connectDatabase();

  app.listen(PORT, '0.0.0.0', () => {
    logger.info(`Cap Table & Fund Management service running on port ${PORT}`);
    logger.info(`Environment: ${process.env.NODE_ENV || 'development'}`);
    if (process.env.CARTA_CLIENT_ID) {
      logger.info('Carta integration configured');
    }
  });
};

// Error handling
process.on('unhandledRejection', (err) => {
  logger.error('Unhandled rejection:', err);
});

process.on('uncaughtException', (err) => {
  logger.error('Uncaught exception:', err);
  process.exit(1);
});

// Start the service
startServer();
