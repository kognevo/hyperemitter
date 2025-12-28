import test from 'tape'
import HyperEmitter from '../hyperemitter.js'
import protobuf from 'protocol-buffers'
import memdb from 'memdb'
import { readFileSync } from 'node:fs'
import { join, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'

const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)
const basicProto = readFileSync(join(__dirname, 'fixture', 'basic.proto'))

test('standalone works', (t) => {
  t.plan(5)

  const emitter = new HyperEmitter(memdb(), basicProto)

  const test1 = {
    foo: 'hello',
    num: 42
  }

  const test2 = {
    bar: 'world',
    id: 23
  }

  let count = 2

  emitter.emit('Test1', test1, (err) => {
    t.error(err, 'no error')
  })

  emitter.emit('Test2', test2, (err) => {
    t.error(err, 'no error')
  })

  emitter.on('Test1', (msg) => {
    t.deepEqual(msg, test1, 'Test1 event matches')
    release()
  })

  emitter.on('Test2', (msg, cb) => {
    t.deepEqual(msg, test2, 'Test2 event matches')

    // second argument can be a function, backpressure is supported
    cb()
    release()
  })

  function release () {
    if (--count === 0) {
      emitter.close(() => t.pass('closed successfully'))
    }
  }
})

test('using registerCodec works', (t) => {
  t.plan(5)

  const codecs = protobuf(basicProto)
  const emitter = new HyperEmitter(memdb())

  emitter.registerCodec('Test1', codecs.Test1)
         .registerCodec('Test2', codecs.Test2)

  const test1 = {
    foo: 'hello',
    num: 42
  }

  const test2 = {
    bar: 'world',
    id: 23
  }

  let count = 2

  emitter.emit('Test1', test1, (err) => {
    t.error(err, 'no error')
  })

  emitter.emit('Test2', test2, (err) => {
    t.error(err, 'no error')
  })

  emitter.on('Test1', (msg) => {
    t.deepEqual(msg, test1, 'Test1 event matches')
    release()
  })

  emitter.on('Test2', (msg) => {
    t.deepEqual(msg, test2, 'Test2 event matches')
    release()
  })

  function release () {
    if (--count === 0) {
      emitter.close(() => t.pass('closed successfully'))
    }
  }
})

test('registerCodec supports objects', (t) => {
  t.plan(5)

  const codecs = protobuf(basicProto)
  const emitter = new HyperEmitter(memdb())

  emitter.registerCodec(codecs)

  const test1 = {
    foo: 'hello',
    num: 42
  }

  const test2 = {
    bar: 'world',
    id: 23
  }

  let count = 2

  emitter.emit('Test1', test1, (err) => {
    t.error(err, 'no error')
  })

  emitter.emit('Test2', test2, (err) => {
    t.error(err, 'no error')
  })

  emitter.on('Test1', (msg) => {
    t.deepEqual(msg, test1, 'Test1 event matches')
    release()
  })

  emitter.on('Test2', (msg) => {
    t.deepEqual(msg, test2, 'Test2 event matches')
    release()
  })

  function release () {
    if (--count === 0) {
      emitter.close(() => t.pass('closed successfully'))
    }
  }
})

test('registerCodec supports arrays', (t) => {
  t.plan(5)

  const codecs = protobuf(basicProto)
  const emitter = new HyperEmitter(memdb())

  emitter.registerCodec([
    { name: 'Test1', codec: codecs.Test1 },
    { name: 'Test2', codec: codecs.Test2 }
  ])

  const test1 = {
    foo: 'hello',
    num: 42
  }

  const test2 = {
    bar: 'world',
    id: 23
  }

  let count = 2

  emitter.emit('Test1', test1, (err) => {
    t.error(err, 'no error')
  })

  emitter.emit('Test2', test2, (err) => {
    t.error(err, 'no error')
  })

  emitter.on('Test1', (msg) => {
    t.deepEqual(msg, test1, 'Test1 event matches')
    release()
  })

  emitter.on('Test2', (msg) => {
    t.deepEqual(msg, test2, 'Test2 event matches')
    release()
  })

  function release () {
    if (--count === 0) {
      emitter.close(() => t.pass('closed successfully'))
    }
  }
})

test('paired works', (t) => {
  t.plan(7)

  const emitter1 = new HyperEmitter(memdb(), basicProto)

  const emitter2 = new HyperEmitter(memdb(), basicProto)

  emitter1.listen(9901, (err) => {
    t.error(err, 'no error')

    emitter2.connect(9901, '127.0.0.1', (err) => {
      t.error(err, 'no error')
    })
  })

  const test1 = {
    foo: 'hello',
    num: 42
  }

  const test2 = {
    bar: 'world',
    id: 23
  }

  let count = 2

  emitter2.on('Test1', (msg) => {
    t.deepEqual(msg, test1, 'Test1 event matches')
    release()
  })

  emitter1.on('Test2', (msg) => {
    t.deepEqual(msg, test2, 'Test2 event matches')
    release()
  })

  emitter1.emit('Test1', test1, (err) => {
    t.error(err, 'no error')
  })

  emitter2.emit('Test2', test2, (err) => {
    t.error(err, 'no error')
  })

  function release () {
    if (--count === 0) {
      emitter1.close(() => {
        emitter2.close(() => t.pass('closed successfully'))
      })
    }
  }
})

test('three way works', (t) => {
  t.plan(9)

  const emitter1 = new HyperEmitter(memdb(), basicProto)

  const emitter2 = new HyperEmitter(memdb(), basicProto)

  const emitter3 = new HyperEmitter(memdb(), basicProto)

  emitter1.listen(9901, '127.0.0.1', (err) => {
    t.error(err, 'no error')

    emitter2.connect(9901, '127.0.0.1', (err) => {
      t.error(err, 'no error')

      emitter2.listen(9902, '127.0.0.1', (err) => {
        t.error(err, 'no error')

        emitter3.connect(9902, '127.0.0.1', (err) => {
          t.error(err, 'no error')
        })
      })
    })
  })

  const test1 = {
    foo: 'hello',
    num: 42
  }

  const test2 = {
    bar: 'world',
    id: 23
  }

  let count = 2

  emitter3.on('Test1', (msg) => {
    t.deepEqual(msg, test1, 'Test1 event matches')
    release()
  })

  emitter3.on('Test2', (msg) => {
    t.deepEqual(msg, test2, 'Test2 event matches')
    release()
  })

  emitter1.emit('Test1', test1, (err) => {
    t.error(err, 'no error')
  })

  emitter1.emit('Test2', test2, (err) => {
    t.error(err, 'no error')
  })

  function release () {
    if (--count === 0) {
      emitter1.close(() => {
        emitter2.close(() => {
          emitter3.close(() => t.pass('closed successfully'))
        })
      })
    }
  }
})

test('remove listeners', (t) => {
  t.plan(2)

  const emitter = new HyperEmitter(memdb(), basicProto)

  const test1 = {
    foo: 'hello',
    num: 42
  }

  emitter.on('Test1', onEvent)
  emitter.removeListener('Test1', onEvent)

  emitter.emit('Test1', test1, (err) => {
    t.error(err, 'no error')
    emitter.close(() => t.pass('closed successfully'))
  })

  function onEvent (msg, cb) {
    t.fail('this should never be called')
  }
})

test('offline peer sync', (t) => {
  t.plan(8)

  const emitter1 = new HyperEmitter(memdb(), basicProto)

  const emitter2db = memdb()

  const emitter2 = new HyperEmitter(emitter2db, basicProto)

  emitter1.listen(9901, (err) => {
    t.error(err, 'no error')

    emitter2.connect(9901, '127.0.0.1', (err) => {
      t.error(err, 'no error')
    })
  })

  const test1 = {
    foo: 'hello',
    num: 42
  }

  const test2 = {
    bar: 'world',
    id: 23
  }

  emitter1.emit('Test1', test1, (err) => {
    t.error(err, 'no error')
  })

  const oldClose = emitter2db.close
  emitter2db.close = (cb) => {
    return cb()
  }

  emitter2.on('Test1', (msg) => {
    t.deepEqual(msg, test1, 'Test1 event matches')

    emitter2.close(() => {
      emitter2db.close = oldClose
      let emitter2New = new HyperEmitter(emitter2db, basicProto)

      emitter1.emit('Test2', test2, (err) => {
        t.error(err, 'no error')
      })

      emitter2New.on('Test2', (msg) => {
        t.deepEqual(msg, test2, 'Test2 event matches')

        emitter1.close(() => {
          emitter2New.close(() => t.pass('closed successfully'))
        })
      })

      emitter2New.connect(9901, '127.0.0.1', (err) => {
        t.error(err, 'no error')
      })
    })
  })
})

test('offline reconnect', (t) => {
  t.plan(7)

  const emitter1 = new HyperEmitter(memdb(), basicProto)

  const emitter2db = memdb()

  const emitter2 = new HyperEmitter(emitter2db, basicProto)

  emitter1.listen(9901, (err) => {
    t.error(err, 'no error')

    emitter2.connect(9901, '127.0.0.1', (err) => {
      t.error(err, 'no error')
    })
  })

  const test1 = {
    foo: 'hello',
    num: 42
  }

  const test2 = {
    bar: 'world',
    id: 23
  }

  emitter1.emit('Test1', test1, (err) => {
    t.error(err, 'no error')
  })

  const oldClose = emitter2db.close
  emitter2db.close = (cb) => {
    return cb()
  }

  emitter2.on('Test1', (msg) => {
    t.deepEqual(msg, test1, 'Test1 event matches')

    emitter2.close(() => {
      emitter2db.close = oldClose
      let emitter2New = new HyperEmitter(emitter2db, basicProto)

      emitter1.emit('Test2', test2, (err) => {
        t.error(err, 'no error')
      })

      emitter2New.on('Test2', (msg) => {
        t.deepEqual(msg, test2, 'Test2 event matches')

        emitter1.close(() => {
          emitter2New.close(() => t.pass('closed successfully'))
        })
      })
    })
  })
})

test('automatically reconnects', (t) => {
  t.plan(7)

  const emitter1 = new HyperEmitter(memdb(), basicProto)

  const emitter2 = new HyperEmitter(memdb(), basicProto, {
    reconnectTimeout: 10
  })

  emitter1.listen(9901, (err) => {
    t.error(err, 'no error')

    emitter2.connect(9901, '127.0.0.1', (err) => {
      t.error(err, 'no error')
    })
  })

  const test1 = {
    foo: 'hello',
    num: 42
  }

  const test2 = {
    bar: 'world',
    id: 23
  }

  emitter1.emit('Test1', test1, (err) => {
    t.error(err, 'no error')
  })

  emitter2.on('Test1', (msg) => {
    t.deepEqual(msg, test1, 'Test1 event matches')

    // using internal data to fake a connection failure
    emitter2._peers['127.0.0.1:9901'].destroy()

    setImmediate(() => {
      emitter1.emit('Test2', test2, (err) => {
        t.error(err, 'no error')
      })

      emitter2.on('Test2', (msg) => {
        t.deepEqual(msg, test2, 'Test2 event matches')

        emitter1.close(() => {
          emitter2.close(() => t.pass('closed successfully'))
        })
      })
    })
  })
})

test('do not re-emit old events', (t) => {
  t.plan(3)

  const db = memdb()
  let emitter = new HyperEmitter(db, basicProto)

  const test1 = {
    foo: 'hello',
    num: 42
  }

  emitter.emit('Test1', test1, (err) => {
    t.error(err, 'no error')
  })

  const oldClose = db.close
  db.close = (cb) => {
    return cb()
  }

  emitter.on('Test1', (msg) => {
    t.deepEqual(msg, test1, 'Test1 event matches')

    emitter.close(() => {
      db.close = oldClose
      emitter = new HyperEmitter(db, basicProto)

      emitter.on('Test1', () => {
        t.fail('this should not happen')
      })

      // timeout needed to wait for the Test1 event to
      // be eventually emitted
      setTimeout(() => {
        emitter.close(() => t.pass('closed successfully'))
      }, 100)
    })
  })
})

test('as stream', (t) => {
  t.plan(6)

  const emitter = new HyperEmitter(memdb(), basicProto)
  const stream = emitter.stream()

  const test1 = {
    foo: 'hello',
    num: 42
  }

  const test2 = {
    bar: 'world',
    id: 23
  }

  let count = 2

  emitter.emit('Test1', test1, (err) => {
    t.error(err, 'no error')

    stream.end({
      name: 'Test2',
      payload: test2
    }, (err) => {
      t.error(err, 'no error')
    })
  })

  stream.once('data', (msg) => {
    t.deepEqual(msg, {
      name: 'Test1',
      payload: test1
    }, 'Test1 event matches')

    stream.once('data', (msg) => {
      t.deepEqual(msg, {
        name: 'Test2',
        payload: test2
      }, 'Test2 event matches')
    })
    release()
  })

  emitter.on('Test2', (msg, cb) => {
    t.deepEqual(msg, test2, 'Test2 event matches')

    // second argument can be a function, backpressure is supported
    cb()
    release()
  })

  function release () {
    if (--count === 0) {
      emitter.close(() => t.pass('closed successfully'))
    }
  }
})

test('as stream starting from a certain point', (t) => {
  t.plan(3)

  const emitter = new HyperEmitter(memdb(), basicProto)

  const test1 = {
    foo: 'hello',
    num: 42
  }

  const test2 = {
    bar: 'world',
    id: 23
  }

  emitter.on('Test1', (msg, cb) => {
    const stream = emitter.stream()

    emitter.emit('Test2', test2)

    stream.once('data', (msg) => {
      t.deepEqual(msg, {
        name: 'Test2',
        payload: test2
      }, 'Test2 event matches')

      emitter.close(() => t.pass('closed successfully'))
    })
  })

  emitter.emit('Test1', test1, (err) => {
    t.error(err, 'no error')
  })
})

test('as stream starting from the beginning', (t) => {
  t.plan(4)

  const emitter = new HyperEmitter(memdb(), basicProto)

  const test1 = {
    foo: 'hello',
    num: 42
  }

  const test2 = {
    bar: 'world',
    id: 23
  }

  emitter.on('Test1', (msg, cb) => {
    const stream = emitter.stream({ from: 'beginning' })

    emitter.emit('Test2', test2)
    stream.once('data', (msg) => {
      t.deepEqual(msg, {
        name: 'Test1',
        payload: test1
      }, 'Test1 event matches')

      stream.once('data', (msg) => {
        t.deepEqual(msg, {
          name: 'Test2',
          payload: test2
        }, 'Test2 event matches')

        emitter.close(() => t.pass('closed successfully'))
      })
    })
  })

  emitter.emit('Test1', test1, (err) => {
    t.error(err, 'no error')
  })
})

test('no eventpeer if it is not needed', (t) => {
  t.plan(3)

  const db = memdb()

  let emitter = new HyperEmitter(db, basicProto)

  emitter.listen(9901, (err) => {
    t.error(err, 'no error')

    const oldClose = db.close
    db.close = (cb) => {
      return cb()
    }

    emitter.close(() => {
      db.close = oldClose
      emitter = new HyperEmitter(db, basicProto)

      emitter.on('EventPeer', (msg) => {
        t.fail('EventPeer should never be emitted')
      })

      emitter.listen(9901, (err) => {
        t.error(err, 'no error')

        // wait some time for the event to be published
        setTimeout(() => {
          emitter.close(() => t.pass('closed successfully'))
        }, 50)
      })
    })
  })
})

test('not idempotent', (t) => {
  t.plan(5)

  const emitter = new HyperEmitter(memdb(), basicProto)

  const test1 = {
    foo: 'hello',
    num: 42
  }

  let count = 2

  emitter.emit('Test1', test1, (err) => {
    t.error(err, 'no error')
  })

  emitter.emit('Test1', test1, (err) => {
    t.error(err, 'no error')
  })

  emitter.on('Test1', (msg) => {
    t.deepEqual(msg, test1, 'Test1 event matches')
    release()
  })

  function release () {
    if (--count === 0) {
      emitter.close(() => t.pass('closed successfully'))
    }
  }
})
