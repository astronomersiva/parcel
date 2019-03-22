// @flow strict-local

import assert from 'assert';
import PromiseQueue from '../src/PromiseQueue';
import {makeDeferredWithPromise} from '../src/Deferred';

describe('PromiseQueue', () => {
  it('add() returns a promise that resolves when that job has completed', async () => {
    let events = [];

    let {
      promise: onePromise,
      deferred: oneDeferred
    } = makeDeferredWithPromise();
    let {
      promise: twoPromise,
      deferred: twoDeferred
    } = makeDeferredWithPromise();

    let queue = new PromiseQueue(arg => (arg === 1 ? onePromise : twoPromise));
    queue.add(1).then(() => events.push('1'));
    queue.add(2).then(() => events.push('2'));

    let runPromise = queue.run().then(() => events.push('run'));

    Promise.resolve()
      .then(() => {
        twoDeferred.resolve();
      })
      .then(() => {
        oneDeferred.resolve();
      });

    await runPromise;

    assert.deepEqual(events, ['2', '1', 'run']);
  });

  it('run() returns a promise that resolves when all jobs have completed', async () => {
    let queue = new PromiseQueue(() => Promise.resolve());
    queue.add(1);
    queue.add(2);

    await queue.run();
  });

  it('run() returns a promise that rejects if any of the jobs reject', async () => {
    let queue = new PromiseQueue(
      arg => (arg === 1 ? Promise.resolve() : Promise.reject())
    );
    queue.add(1);
    queue.add(2);

    // TODO: Use assert.rejects when we only support Node 10+
    let err;
    try {
      await queue.run();
    } catch (e) {
      err = e;
    }

    assert(err != null);
  });
});
