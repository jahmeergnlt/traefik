"use strict";

const { Mutex } = require('async-mutex');

class Server {
  constructor() {
    this.mutex = new Mutex();
    this.currentConfig = {};
    this.activeHandler = null;
    this.providersData = new Map();
  }

  async updateProvider(providerId, config) {
    const release = await this.mutex.acquire();
    try {
      this.providersData.set(providerId, config);
      
      let mergedConfig = {};
      for (const [, cfg] of this.providersData) {
        mergedConfig = { ...mergedConfig, ...cfg };
      }

      this.currentConfig = mergedConfig;
      this.activeHandler = this.buildHandler(mergedConfig);
    } finally {
      release();
    }
  }

  buildHandler(config) {
    return {
      execute: (req) => {
        return {
          router: config.router,
          middleware: config.middleware
        };
      }
    };
  }

  async handleRequest(req) {
    const release = await this.mutex.acquire();
    try {
      return this.activeHandler ? this.activeHandler.execute(req) : null;
    } finally {
      release();
    }
  }

  getConfig() {
    return this.currentConfig;
  }
}

module.exports = Server;
