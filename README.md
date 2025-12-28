# HyperEmitter &nbsp;&nbsp;[![Build Status](https://travis-ci.org/mcollina/hyperemitter.png)](https://travis-ci.org/mcollina/hyperemitter)

HyperEmitter is a horizontally scalable __and__ persistent EventEmitter powered by a [Merkle DAG](http://npm.im/hyperlog).
(Yes, it's like a blockchain). In other contexts, this concept is also called an EventStore. HyperEmitter
uses [protocol-buffers](https://developers.google.com/protocol-buffers/), specifically
[mafintosh's](https://github.com/mafintosh/protocol-buffers) implementation, for handling message schemas, although custom codecs are also supported.

> This module is __highly experimental__, __possibly under-perfoming__, and __may have bugs__, use with
> caution. On the other end, if the thought of a persistent, horizontally scaleable EventStore gets you
> excite please jump in wherever you feel you can help!

## Installation

### Node.js (ESM or CommonJS)

To install the module for use locally in your project:

```bash
npm install hyperemitter
```

**Requirements:** Node.js 20+ (LTS)

### Bun

HyperEmitter is fully compatible with Bun runtime:

```bash
bun add hyperemitter
```

To install the companion CLI tool globally:

```bash
npm install -g hyperemitter
# or with Bun
bun install -g hyperemitter
```

## Module System Support

HyperEmitter supports both ESM (ECMAScript Modules) and CommonJS:

### ESM (Recommended)

```javascript
import HyperEmitter from 'hyperemitter'
import memdb from 'memdb'
import { readFileSync } from 'node:fs'
import { join, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'

const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)

const schema = readFileSync(join(__dirname, 'example-schema.proto'))
const emitter = new HyperEmitter(memdb(), schema)
```

### CommonJS

```javascript
// Note: For best compatibility, use ESM or dynamic import
const HyperEmitter = (await import('hyperemitter')).default
const memdb = (await import('memdb')).default
// ... rest of your code
```

### Bun

Bun natively supports ESM, so you can use the ESM syntax directly:

```javascript
import HyperEmitter from 'hyperemitter'
import memdb from 'memdb'
// ... rest of your code
```

## Example

The example below demonstrates how to connect two HyperEmitters together and how they both receive all messages sent.

### ESM Example

```javascript
import HyperEmitter from 'hyperemitter'
import memdb from 'memdb'
import { readFileSync } from 'node:fs'
import { join, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'

const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)

// Use the example-schema.proto as the message schema
const schema = readFileSync(join(__dirname, 'example-schema.proto'))

// Two emitters will be used for this example, notice each
// maintains its own leveldb store and share the same schema
const emitterOne = new HyperEmitter(memdb('a'), schema)
const emitterTwo = new HyperEmitter(memdb('b'), schema)

// Listen on port 9901, ensure no connection error
emitterOne.listen(9901, (err) => {
  if (err) return

  // Connect to the first emitter
  emitterTwo.connect(9901, '127.0.0.1', (err) => {
    if (err) return
  })

  // Basic message types
  const userAddedMsg = {
    id: 1,
    username: 'user'
  }

  const userRemovedMsg = {
    id: 1
  }

  // Messages sent on either emitter will be handled
  emitterOne.on('userRemoved', (msg) => {
    console.log('userRemoved: ', msg)
  })

  // Messages sent on either emitter will be handled
  emitterTwo.on('userAdded', (msg) => {
    console.log('userAdded: ', msg)
  })

  // We send each message across the opposite emitter
  emitterOne.emit('userAdded', userAddedMsg)
  emitterTwo.emit('userRemoved', userRemovedMsg)

  // Clean up after 500ms
  setTimeout(() => {
    emitterOne.close()
    emitterTwo.close()
  }, 500)
})
```

## Using The CLI Tool
HyperEmitter comes with a nice CLI to work with other remote HyperEmitter's. Also, it doubles
as really useful tool for debugging. The sample below uses the schema in `test/fixture/`.

```
hypem --db db --port 1234 fixture/messages.proto
```

On pressing enter, you should see output similar to the following:

```
listening on 1234 127.0.0.1
EventPeer  { id: '123', addresses: [ { ip: 'localhost', port: 1234 } ] }
```

From here you can then use the provided REPL to work with HyperEmitter:

```javascript
hyper.emit('Hello', { from: 'Matteo' })
```

And receive the following response:

```
Hello { from: 'Matteo', message: '' }
```

To connect another hyperEmitter in another shell:

```
hypem --db db2 --target-port 1234 fixture/messages.proto
```

Which will output the following:

```
connected to localhost 1234
EventPeer { id: '123', addresses: [ { ip: 'localhost', port: 1234 } ] }
Hello { from: 'Matteo', message: '' }
```

As you can see the events are synced up!

To export the data in [newline delimited json](http://ndjson.org/), use:

```
hypem --no-repl --db db4 --target-port 1234 fixture/messages.proto
```

Which will produce output like so:

```
{"name":"EventPeer","payload":{"id":"123","addresses":[{"ip":"localhost","port":1234}]}}
{"name":"Hello","payload":{"from":"Matteo","message":""}}
```

The cli tool also works as a input stream, following the UNIX philosophy. If you close a REPL or a ndjson stream,
the next time it will start where it stopped. If you have a stream, you can start back from the beginning using
the `--from-scratch` flag.

## API Reference
This page contains a list of public API's exposed by the HyperEmitter module as well as a brief
description of their use. For additional samples please checkout out our [examples](./examples/)
folder.

  * <a href="#hyperemitter">HyperEmitter</a>
  * <a href="#messages">.messages</a>
  * <a href="#registerCodec">.registerCodec()</a>
  * <a href="#emit">.emit()</a>
  * <a href="#on">.on()</a>
  * <a href="#removeListener">.removeListener()</a>
  * <a href="#connect">.connect()</a>
  * <a href="#listen">.listen()</a>
  * <a href="#stream">.stream()</a>
  * <a href="#close">.close()</a>

<a name="hyperemitter"></a>
### HyperEmitter(db, schema, [opts])
HyperEmitter is both the name of the module and of the function to be required. The function is actually
a class or a class factory depending on how it is used. The code below shows both valid ways to get a
new instance of HyperEmitter for use:

```
var emitterOne = Hyperemitter(someDb, someSchema)
var emitterTwo = new HyperEmitter(someDb, someSchema)
```

 * ##### _db_
 The `db` argument accepts a [levelup](http://npm.im/levelup) instance, which in turn is powered by
 [leveldb](). We recommend [level](http://npm.im/level) for persistent storage and
 [memdb](http://npm.im/memdb) if you require an in memory store.

 * ##### _schema_
 An string or stream that represents all of the messages to support in a given instance of HyperEmitter.

 * ##### _opts_
 An optional object that can be provided to configure the created instance. All available
 options are listed below.

  * ___reconnectTimeout:___ The timeout that this instance will wait before reconnecting to peers.

---

### .messages <a name="messages"></a>
Message types are stored in the `.messages` field for each instance created. This field is populated
with a normalized object based on the schema provided. Each property represents a single message, indvidual
messages, as well as their encoder and decoder can be accessed from here.

``` js
var emitter = HyperEmitter(someDb, someSchema)

// the object containing each message as a property
var messages = emitter.messages

// access an individual message by it's name
var message = messages[messageName]

// access a given message's encode / decode functions
var encoder = message.encode
var decode = message.decode
```

---

### .emit(event, message, [callback]) <a name="emit"></a>
Messages can be emitted from HyperEmitter using the `.emit()` method. This method takes the name of the
event to be emitted and validates `message` against the schema before sending it off to any listening
subscribers, in parallel. Once complete the `callback` function is called, if present.

 * ##### _event_
 The name of one of the message definitions from the provided schema.

 * ##### _message_
 Any object who's shape matches the named event. It's keys are validated against the schema.

 * ##### _callback_
 An optional function that will be called once the emitted message has been added to the log.

---

### .registerCodec(name, codec | codecs) <a name="registerCodec"></a>
Custom codecs can be registered as long as they have both an `encode` and `decode` method. Codecs
are keyed by message name. For ease of use registration params can be provided as args (name,
codec), an object, or an array of `{name: '', codec: obj}`. Only once codec can be registered against one message at any given time.

 * ##### _name_
 The name of the message message this codec handles.

 * ##### _codec_
 Any object which has an `encode` and `decode` method.

 * ##### _codecs_
 An object or array which represents a collection of codecs and their names.

---

<a name="on"></a>
### .on(event, callback(message[, done]))
Subscribes to and provides a function to be called each time and event is raised.

 * ##### _event_
 The name of the event being subscribed to.

 * ##### _callback_
 The function to be called when a new event is raised. The `message` arg holds the message emitted. The
 `done` arg can be used for letting the emitter know when the function has completed the handling of
 the event.

---

<a name="removeListener"></a>
### .removeListener(event, callback(message[, done]))
Removes the listener who matches the one provided. This method does not work with anonymous functions, only a
function with a prior reference can be removed.

 * ##### _event_
 The name of the event the listener is subscribed to.

 * ##### _callback_
 The reference of the function originally used in the `.on` call.

---

<a name="connect"></a>
### .connect(port, host[, done])
Connects this HyperEmitter with another one which we call a peer. Peers can exist on other machines, HyperEmitter
will communicate over TCP using the `host` and `port` provided. An optional function can be provided that will be
called once the connection has been made.

 * ##### _port_
 The port of the machine the peer to connect to resides on

 * ##### _host_
 The host of the machine the peer to connect to resides on.

 * ##### _done_
 A function to be called when connected.

---

<a name="listen"></a>
### .listen(port[, host[, done]])
Listen on a given port/host combination. An `EventPeer` event will be
emitter.

 * ##### _port_
 The port to listen on.

 * ##### _host_
 The host to listen on.

 * ##### _done_
 An optional function to be called one listening has begun.

---

<a name="stream"></a>
### .stream([opts])
A Duplex stream to emit and receive events.

 * ##### _opts_
 An optional object of settings.

  * `from:` The point in the stream to return events since. supports 'beginning'

---

<a name="close"></a>
### .close(callback)
Close a given HyperEmitter. After, all `emit` will return an error.

 * ##### _callback_

 An optional function to be called when close has completed.


---

## Modernization (v2.0.0)

This module has been modernized for 2025 with:

- **ESM Support**: Native ES Modules with dual CommonJS compatibility
- **Modern Dependencies**: All dependencies upgraded to latest versions
- **Bun Runtime Support**: Full compatibility with Bun runtime
- **Modern JavaScript**: Uses const/let, arrow functions, and modern patterns
- **Node.js 20+**: Requires Node.js 20 or higher (LTS)

### Migration from v1.x

If you're upgrading from v1.x:

1. **ESM Users**: No changes needed, continue using `import HyperEmitter from 'hyperemitter'`
2. **CommonJS Users**: Consider migrating to ESM, or use dynamic import: `const HyperEmitter = (await import('hyperemitter')).default`
3. **Node.js Version**: Ensure you're running Node.js 20+

## Contributing

HyperEmitter is a mad science project and like all mad science projects it requires mad scientists, the more
the merrier! If you feel you can help in any way, be it with examples, extra testing, or new features please
be our guest. See our [Contribution Guide](./CONTRIBUTING.md) for information on obtaining the source and an overview of the
tooling used.

[![js*standard-style](https://raw.githubusercontent.com/feross/standard/master/badge.png)](https://github.com/feross/standard)

## License

Copyright Matteo Collina 2015, Licensed under [ISC](./LICENSE)
