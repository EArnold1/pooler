type Connection = {
  id: number;
  fn: () => void;
  lastUsed: number | null;
};

type Waiter = {
  resolve: (value: unknown) => void;
  reject: (reason?: any) => void;
  timer: number | null;
};

type PoolerOpts = {
  min_idle_connections?: number;
  max_connections?: number;
};

const MIN_IDLE_CONNECTIONS = 4;
const MAX_CONNECTIONS = 10;

class ConnPooler {
  idle: Array<Connection> = [];
  min_idle_connections: number;
  busy = new Set<Connection>();
  total = 0;
  idle_timeout = 15000; // 15s
  waiting_queue: Array<Waiter> = [];
  max_connections: number;
  acquire_timeout = 10000; // 10s
  idle_checker: any = 0;

  constructor(opts: PoolerOpts) {
    this.min_idle_connections =
      opts?.min_idle_connections ?? MIN_IDLE_CONNECTIONS;
    this.max_connections = opts?.max_connections ?? MAX_CONNECTIONS;

    this.start();
  }

  acquire(): Connection | Promise<unknown> {
    if (this.idle.length) {
      const conn = this.idle.pop();
      if (!conn) throw new Error("conn not found");
      this.busy.add(conn);
      return conn;
    }

    if (this.total < this.max_connections) {
      this.total++;

      const conn = this.createConn();
      this.busy.add(conn);
      return conn;
    }

    return new Promise((resolve, reject) => {
      const waiter: Waiter = { resolve, reject, timer: null };

      waiter.timer = setTimeout(() => {
        const i = this.waiting_queue.indexOf(waiter);
        if (i !== -1) this.waiting_queue.splice(i, 1); // remove from the queue
        reject(new Error("acquire timeout"));
      }, this.acquire_timeout);

      this.waiting_queue.push(waiter);
    });
  }

  release(conn: Connection) {
    if (!this.busy.has(conn)) return;
    this.busy.delete(conn);

    // handle broken connections
    // close connection

    if (this.waiting_queue.length) {
      const waiter = this.waiting_queue.shift();
      if (!waiter) throw new Error("Waiter not found");
      clearTimeout(waiter.timer!);
      this.busy.add(conn);
      waiter.resolve(conn.fn());
      return;
    }

    this.idle.push({ ...conn, lastUsed: Date.now() });
  }

  // should be an actual connection to a db
  private createConn(): Connection {
    return {
      id: this.total,
      fn: () => {},
      lastUsed: null,
    };
  }

  private start() {
    this.idle_checker = setInterval(() => this.idle_check_runner(), 10_000);
    this.idle_checker.unref(); // don't let this timer keep the process alive
  }

  idle_check_runner() {
    const now = Date.now();

    this.idle = this.idle.filter((conn) => {
      const expired = conn?.lastUsed
        ? now - conn.lastUsed > this.idle_timeout &&
          this.total > this.min_idle_connections
        : false;

      if (expired) this.destroy(conn);
      return !expired;
    });
  }

  private destroy(conn: Connection) {
    this.idle = this.idle.filter((c) => c.id === conn.id);
    this.busy.delete(conn);
    // in a real connection, close the conn
    this.total--;
  }
}
