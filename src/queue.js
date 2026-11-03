import { randomUUID } from 'node:crypto';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const rand = (a, b) => Math.floor(a + Math.random() * (b - a + 1));

/**
 * Single-lane queue. One WhatsApp connection must send one message at a time
 * to look human, so the HTTP layer never blocks: requests are accepted
 * instantly and drained here.
 *
 *  - Idle queue  -> message leaves immediately (after the typing simulation).
 *  - Burst       -> messages are spaced by a random gap (GAP_MIN..GAP_MAX).
 */
export class SendQueue {
  constructor(sendFn, { gapMin = 2000, gapMax = 4000, maxLength = 200, maxRetries = 2 } = {}) {
    this.sendFn = sendFn;
    this.gapMin = gapMin;
    this.gapMax = gapMax;
    this.maxLength = maxLength;
    this.maxRetries = maxRetries;
    this.pending = [];
    this.jobs = new Map();
    this.running = false;
    this.lastSentAt = 0;
  }

  get size() {
    return this.pending.length;
  }

  enqueue(data) {
    if (this.pending.length >= this.maxLength) {
      throw Object.assign(new Error('Queue is full, try again shortly'), { status: 429 });
    }
    const job = { id: randomUUID(), data, status: 'queued', attempts: 0, createdAt: Date.now() };
    job.promise = new Promise((resolve, reject) => {
      job.resolve = resolve;
      job.reject = reject;
    });
    job.promise.catch(() => {}); // avoid unhandled rejection when nobody awaits
    this.jobs.set(job.id, job);
    this.pending.push(job);
    this.#trim();
    this.#run();
    return job;
  }

  getJob(id) {
    const j = this.jobs.get(id);
    return j && { id: j.id, status: j.status, attempts: j.attempts, error: j.error, result: j.result };
  }

  #trim() {
    if (this.jobs.size <= 500) return;
    for (const [id, j] of this.jobs) {
      if (j.status === 'sent' || j.status === 'failed') this.jobs.delete(id);
      if (this.jobs.size <= 400) break;
    }
  }

  async #run() {
    if (this.running) return;
    this.running = true;
    while (this.pending.length) {
      const job = this.pending.shift();
      const wait = this.lastSentAt + rand(this.gapMin, this.gapMax) - Date.now();
      if (wait > 0) await sleep(wait);

      job.status = 'sending';
      job.attempts += 1;
      try {
        job.result = await this.sendFn(job.data);
        job.status = 'sent';
        this.lastSentAt = Date.now();
        job.resolve(job.result);
      } catch (err) {
        if (!err.permanent && job.attempts <= this.maxRetries) {
          job.status = 'retrying';
          await sleep(2000 * job.attempts);
          this.pending.unshift(job);
        } else {
          job.status = 'failed';
          job.error = err.message;
          job.reject(err);
        }
      }
    }
    this.running = false;
  }
}
