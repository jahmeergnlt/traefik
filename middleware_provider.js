class ProviderManager {
  constructor(initialMiddleware = []) {
    this.middleware = [...initialMiddleware];
    this.chain = this.compileChain(this.middleware);
    this.lock = Promise.resolve();
  }

  compileChain(mwList) {
    return async (context, next) => {
      let index = -1;
      const dispatch = async (i) => {
        if (i <= index) throw new Error('next() called multiple times');
        index = i;
        let fn = mwList[i];
        if (i === mwList.length) fn = next;
        if (!fn) return;
        return fn(context, () => dispatch(i + 1));
      };
      return dispatch(0);
    };
  }

  async updateMiddleware(newMiddleware) {
    let release;
    const nextLock = new Promise((resolve) => {
      release = resolve;
    });
    const prevLock = this.lock;
    this.lock = nextLock;

    await prevLock;
    try {
      this.middleware = [...newMiddleware];
      this.chain = this.compileChain(this.middleware);
    } finally {
      release();
    }
  }

  async execute(context, next) {
    const currentChain = this.chain;
    return currentChain(context, next);
  }
}