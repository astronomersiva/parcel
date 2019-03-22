// @flow strict-local

import invariant from 'assert';
import nullthrows from 'nullthrows';

import {makeDeferredWithPromise, type Deferred} from './Deferred';

type PromiseQueueOpts = {
  maxConcurrent?: number,
  retry?: boolean // TODO: Allow multiple retries?
};

type Job = {|
  deferred: Deferred<void>,
  promise: Promise<void>,
  retried: boolean
|};

export default class PromiseQueue<TFirstArg, TRestArgs: Array<mixed>, TRet> {
  _deferred: ?Deferred<void> = null;
  _maxConcurrent: number;
  _numRunning: number = 0;
  _process: (first: TFirstArg, ...rest: TRestArgs) => Promise<TRet>;
  _processing: Map<TFirstArg, Job> = new Map();
  _queue: Array<[TFirstArg, TRestArgs]> = [];
  _retry: boolean;
  _runPromise: ?Promise<void> = null;

  constructor(
    callback: (TFirstArg, ...TRestArgs) => Promise<TRet>,
    options: PromiseQueueOpts = {}
  ) {
    if (options.maxConcurrent != null && options.maxConcurrent <= 0) {
      throw new TypeError('maxConcurrent must be a positive, non-zero value');
    }

    this._process = callback;
    this._maxConcurrent =
      options.maxConcurrent == null ? Infinity : options.maxConcurrent;
    this._retry = options.retry !== false;
  }

  // Add a job to the queue. Returns a promise that resolves when that job
  // has been processed.
  add(key: TFirstArg, ...args: TRestArgs): Promise<void> {
    let existingJob = this._processing.get(key);
    if (existingJob) {
      return existingJob.promise;
    }

    let deferredWithPromise = makeDeferredWithPromise();
    let jobPromise = deferredWithPromise.promise;
    this._processing.set(key, {
      deferred: deferredWithPromise.deferred,
      promise: jobPromise,
      retried: false
    });

    if (this._runPromise && this._numRunning < this._maxConcurrent) {
      this._runJob(key, args);
    } else {
      this._queue.push([key, args]);
    }

    // Attach an empty catch handler to suppress any unhandled rejection issues
    // from the environment. This promise is purely informational for the caller,
    // and having it reject shouldn't take down the program. `run()` will also reject
    // in the case this fails, and that should be properly handled.
    // TODO: Maybe shouldn't use promises for this.
    jobPromise.catch(() => {});
    return jobPromise;
  }

  run(): Promise<void> {
    if (this._runPromise) {
      return this._runPromise;
    }

    let {deferred, promise} = makeDeferredWithPromise();
    this._deferred = deferred;
    this._runPromise = promise;

    this._next();

    return promise;
  }

  async _runJob(key: TFirstArg, args: TRestArgs) {
    let job = nullthrows(this._processing.get(key));
    this._numRunning++;
    try {
      await this._process(key, ...args);
    } catch (err) {
      this._numRunning--;
      if (this._retry && !job.retried) {
        this._processing.set(key, {...job, retried: true});
        this._queue.push([key, args]);
        this._next();
      } else {
        this._processing.delete(key);

        job.deferred.reject(err);
        if (this._deferred != null) {
          this._deferred.reject(err);
        }

        this._reset();
      }

      return;
    }

    job.deferred.resolve();
    this._processing.delete(key);
    this._numRunning--;
    this._next();
  }

  _next() {
    if (this._runPromise == null) {
      return;
    }

    if (this._queue.length > 0) {
      while (this._queue.length > 0 && this._numRunning < this._maxConcurrent) {
        this._runJob(...this._queue.shift());
      }
    }

    if (this._processing.size === 0) {
      invariant(this._deferred != null);
      this._deferred.resolve();
      this._reset();
    }
  }

  _reset() {
    this._processing = new Map();
    this._runPromise = null;
    this._deferred = null;
  }
}
