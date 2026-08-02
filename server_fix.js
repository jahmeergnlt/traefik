class ServerManager {
  constructor() {
    this.mu = false;
    this.configVersion = 0;
    this.activeMiddleware = null;
    this.queue = [];
  }

  async updateConfiguration(providerId, middleware, callback) {
    while (this.mu) {
      await new Promise(resolve => this.queue.push(resolve));
    }
    this.mu = true;

    try {
      this.configVersion++;
      const currentVersion = this.configVersion;
      
      // Simulate atomic router and middleware chain build
      this.activeMiddleware = {
        providerId,
        middleware,
        version: currentVersion
      };

      if (callback) {
        callback(this.activeMiddleware);
      }
    } finally {
      this.mu = false;
      if (this.queue.length > 0) {
        const next = this.queue.shift();
        next();
      }
    }
  }

  handleRequest() {
    return this.activeMiddleware;
  }
}

module.exports = ServerManager;