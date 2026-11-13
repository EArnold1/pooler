import { type Client } from "pg";

type Connection = {
  client: Client;
  lastUsed: number;
  destroyed: boolean;
};

type Waiter = {
  resolve: (value: Connection) => void;
  reject: (reason?: any) => void;
  timer: NodeJS.Timeout | null;
};

type PoolerOpts = {
  min_idle_connections?: number;
  max_connections?: number;
};

type ClientFactory = () => Promise<Client>;

const MIN_IDLE_CONNECTIONS = 4;
const MAX_CONNECTIONS = 10;

class Pooler {
  idle: Array<Connection> = [];
  min_idle_connections: number;
  busy = new Set<Connection>();
  total = 0;
  idle_timeout = 15000; // 15s
  waiting_queue: Array<Waiter> = [];
  max_connections: number;
  acquire_timeout = 10000; // 10s
  idle_checker: NodeJS.Timeout | null = null;
  createClient: ClientFactory;

  constructor(opts: PoolerOpts, createClient: ClientFactory) {
    this.min_idle_connections =
      opts?.min_idle_connections ?? MIN_IDLE_CONNECTIONS;
    this.max_connections = opts?.max_connections ?? MAX_CONNECTIONS;
    this.createClient = createClient;

    this.start();
  }

  // Used to get a connection
  async acquire(): Promise<Connection> {
    const conn = this.idle.pop();
    if (conn) {
      this.busy.add(conn);
      return conn;
    }

    if (this.total < this.max_connections) {
      this.total++;

      try {
        const conn = await this.createConn();
        this.busy.add(conn);
        return conn;
      } catch (error) {
        this.total--;
        throw error;
      }
    }

    return new Promise<Connection>((resolve, reject) => {
      const waiter: Waiter = { resolve, reject, timer: null };

      waiter.timer = setTimeout(() => {
        const index = this.waiting_queue.indexOf(waiter);
        if (index !== -1) this.waiting_queue.splice(index, 1); // remove from the queue
        reject(new Error("acquire timeout"));
      }, this.acquire_timeout);

      this.waiting_queue.push(waiter);
    });
  }

  // This function is called after a connection is used
  // After release, it can also assign a waiter in the queue a connection immediately
  release(conn: Connection) {
    if (!this.busy.has(conn)) return;
    this.busy.delete(conn);

    const waiter = this.waiting_queue.shift();
    if (waiter) {
      clearTimeout(waiter.timer!);
      this.busy.add(conn);
      waiter.resolve(conn);
      return;
    }

    conn.lastUsed = Date.now();
    this.idle.push(conn);
  }

  // Creates a new connection and listens on `error` and `end`
  private async createConn(): Promise<Connection> {
    const client = await this.createClient();
    const conn: Connection = { client, lastUsed: Date.now(), destroyed: false };

    client.on("error", (err) => {
      console.error(err);
      this.destroy(conn);
    });
    client.on("end", () => this.destroy(conn));

    try {
      await client.connect();
    } catch (err) {
      conn.destroyed = true;
      throw err;
    }
    return conn;
  }

  private start() {
    this.idle_checker = setInterval(() => this.idle_check_runner(), 10_000);
    this.idle_checker.unref(); // don't let this timer keep the process alive
  }

  // This function clears any idle connection that has passed the `idle_timeout` duration
  idle_check_runner() {
    const now = Date.now();

    // creating a new array, because destroy mutates the original idle list
    for (const conn of [...this.idle]) {
      if (
        now - conn.lastUsed > this.idle_timeout &&
        this.total > this.min_idle_connections
      ) {
        this.destroy(conn);
        conn.client.end().catch(() => {});
      }
    }
  }

  // This function removes a connection either from the idle list or busy set, and it is idempotent
  private destroy(conn: Connection) {
    if (conn.destroyed) return;
    conn.destroyed = true;
    this.total--;

    const index = this.idle.indexOf(conn);
    if (index !== -1) this.idle.splice(index, 1);
    this.busy.delete(conn);

    this.serveWaiters();
  }

  // Service waiter immediately they are available but no idle connection
  // It triggers creating a new connection
  private serveWaiters() {
    if (this.waiting_queue.length + this.total < this.max_connections) {
      const queue = this.waiting_queue.shift()!;
      clearTimeout(queue.timer!);
      this.acquire().then(queue.resolve, queue.reject);
    }
  }

  // close
}

export { Pooler };
