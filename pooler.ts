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

class ConnPoller {
  idle: Array<Connection> = [];
  busy = new Set<Connection>();
  total = 0;
  //   idle_timeout = 5000; // ms
  waiting_queue: Array<Waiter> = [];
  max_connections = 5;
  acquire_timeout = 10000; // ms

  acquire(): Connection | Promise<unknown> {
    if (this.idle.length) {
      const conn = this.idle.pop();
      if (!conn) throw new Error("conn not found"); // throw
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
}
