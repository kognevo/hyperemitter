import { EventEmitter } from 'node:events'
import hyperlog from 'hyperlog'
import { createServer as createNetServer, connect } from 'node:net'
import { createId } from '@paralleldrive/cuid2'
import { networkInterfaces } from 'node:os'
import pump from 'pump'
import fastparallel from 'fastparallel'
import eos from 'end-of-stream'
import bulkws from 'bulk-write-stream'
import through2 from 'through2'
import duplexify from 'duplexify'
import deepEqual from 'deep-equal'
import { readFileSync } from 'node:fs'
import { join, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import protobuf from 'protocol-buffers'

const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)

const noop = () => {}

const STOREID = '!!STOREID!!'
const PEERS = '!!PEERS!!'
const MYEVENTPEER = '!!MYEVENTPEER!!'
const defaults = {
  reconnectTimeout: 1000
}

const coreCodecs = protobuf(readFileSync(join(__dirname, 'codecs.proto')))

function initializeCodecs (codecs) {
  codecs = codecs || []

  if (Buffer.isBuffer(codecs) || typeof codecs === 'string') {
    codecs = protobuf(codecs)
  }

  Object.keys(coreCodecs).forEach((name) => {
    codecs[name] = coreCodecs[name]
  })

  return codecs
}

function createChangeStream () {
  const readStream = this._hyperlog.createReadStream({
    since: this._hyperlog.changes,
    live: true
  })

  return pump(readStream, bulkws.obj(processStream.bind(this)))
}

function processStream (changes, next) {
  this._parallel(this, publish, changes, next)
}

function publish (change, done) {
  const container = this.codecs.Event.decode(change.value)
  const name = container.name
  const decoder = this.codecs[name]
  let event = container.payload

  if (decoder) event = decoder.decode(event)
  this._parallel(this, this._listeners[name] || [], event, done)
}

function createServer () {
  this._server = createNetServer((peerStream) => {
    this.status.emit('peerConnected')

    const peerId = this._lastPeerId++
    const localStream = this._hyperlog.replicate({ live: true })
    const boundStreams = pump(peerStream, localStream, peerStream, (err) => {
      if (err) this.status.emit('peerError', err)
      else delete this._peers[peerId]
    })

    this._peers[peerId] = boundStreams
  })
}

function getLocalAddresses () {
  const ifaces = networkInterfaces()
  return Object.keys(ifaces).reduce((addresses, iface) => {
    return ifaces[iface].filter((ifaceIp) => {
      return !ifaceIp.internal
    }).reduce((addresses, ifaceIp) => {
      addresses.push(ifaceIp)
      return addresses
    }, addresses)
  }, [])
}

function connectToPeer (that, port, host, tries, callback) {
  const stream = connect(port, host)
  const key = `${host}:${port}`

  if (that._peers[key]) {
    return callback ? callback() : undefined
  }

  that._peers[key] = stream

  stream.on('connect', () => {
    const replicate = that._hyperlog.replicate({ live: true })
    pump(replicate, stream, replicate)

    const peers = Object.keys(that._peers).map((key) => {
      const split = key.split(':')
      return { address: split[0], port: split[1] }
    })

    that._db.put(PEERS, JSON.stringify(peers), (err) => {
      if (callback) {
        callback(err)
        callback = null
      }
    })
  })

  eos(stream, (err) => {
    delete that._peers[key]
    if (err) {
      that.status.emit('connectionError', err, stream)
      if (!that._closed && tries < 10) {
        setTimeout(() => {
          connectToPeer(that, port, host, tries + 1, callback)
        }, that._opts.reconnectTimeout)
      } else {
        return callback ? callback(err) : undefined
      }
    }
  })
}

function connectToKnownPeers () {
  this._db.get(PEERS, (err, peers) => {
    if (err && err.notFound) return
    if (err) return this.status.emit('error', err)

    const connectToPeer = (peer, next) => {
      this.connect(peer.port, peer.address, next)
    }

    this._parallel(this, connectToPeer, JSON.parse(peers), noop)
  })
}

function handleNewPeers () {
  this.on('EventPeer', (peer, callback) => {
    const port = peer.addresses[0].port
    const address = peer.addresses[0].ip

    if (peer.id !== peer.id) this.connect(port, address, callback)
    else callback()
  })
}

function destroyOrClose (resource, callback) {
  if (resource.destroy) {
    resource.destroy()
    setImmediate(callback)
  } else {
    resource.close(callback)
  }
}

function HyperEmitter (db, codecs, opts) {
  if (!(this instanceof HyperEmitter)) {
    return new HyperEmitter(db, codecs, opts)
  }

  this._opts = { ...defaults, ...opts }
  this._parallel = fastparallel({ results: false })
  this._db = db
  this._hyperlog = hyperlog(db)

  this._closed = false
  this._listening = false

  this._peers = {}
  this._lastPeerId = 0
  this._listeners = {}

  this.status = new EventEmitter()
  this.codecs = initializeCodecs(codecs)
  this.messages = this.codecs

  createServer.call(this)
  connectToKnownPeers.call(this)
  handleNewPeers.call(this)

  this._hyperlog.ready(() => {
    if (this._closed) return

    this.changeStream = createChangeStream.call(this)
    this.changes = this.changeStream
    this.status.emit('ready')
  })
}

HyperEmitter.prototype.emit = function (name, data, callback) {
  const encoder = this.codecs[name]

  if (encoder) data = encoder.encode(data)
  const container = this.codecs.Event.encode({
    name: name,
    payload: data
  })

  this._hyperlog.append(container, callback)

  return this
}

HyperEmitter.prototype.on = function (name, handler) {
  let toInsert = handler

  if (toInsert.length < 2) {
    toInsert = (msg, callback) => {
      handler(msg)
      callback()
    }

    handler.wrapped = toInsert
  }

  this._listeners[name] = this._listeners[name] || []
  this._listeners[name].push(toInsert)

  return this
}

HyperEmitter.prototype.registerCodec = function (name, codec) {
  if (typeof name === 'string') {
    this.codecs[name] = codec
    return this
  }

  const codecs = name

  if (Array.isArray(codecs)) {
    codecs.forEach((element) => {
      this.codecs[element.name] = element.codec
    })
    return this
  }

  if (typeof codecs === 'object') {
    Object.keys(codecs).forEach((name) => {
      this.codecs[name] = codecs[name]
    })
    return this
  }

  return this
}

HyperEmitter.prototype.removeListener = function (name, func) {
  if (func.wrapped) {
    func = func.wrapped
  }

  this._listeners[name].splice(this._listeners[name].indexOf(func), 1)
  return this
}

HyperEmitter.prototype.getId = function (callback) {
  if (this.id) { return callback(null, this.id) }

  const db = this._db

  db.get(STOREID, (err, value) => {
    if (err && !err.notFound) { return callback(err) }
    this.id = value || createId()
    db.put(STOREID, this.id, (err) => {
      if (err) {
        return callback(err)
      }
      callback(null, this.id)
    })
  })
}

HyperEmitter.prototype.connect = function (port, host, callback) {
  connectToPeer(this, port, host, 1, callback)
}

HyperEmitter.prototype.listen = function (port, address, callback) {
  if (typeof address === 'function') {
    callback = address
    address = null
  }

  this._listening = true

  this.getId((err, id) => {
    if (err) {
      return callback(err)
    }

    this._server.listen(port, address, (err) => {
      if (err) {
        return callback(err)
      }

      const addresses = address ? [{ address: address }] : getLocalAddresses()

      const mappedAddresses = addresses.map((ip) => {
        return {
          ip: ip.address,
          port: this._server.address().port
        }
      })

      const toStore = {
        id: id,
        addresses: mappedAddresses
      }

      this._db.get(MYEVENTPEER, { valueEncoding: 'json' }, (err, value) => {
        if (err && !err.notFound) {
          return callback(err)
        }

        if (deepEqual(value, toStore)) {
          return callback(null, this._server.address())
        }

        this.emit('EventPeer', toStore, (err) => {
          if (err) { return callback(err) }

          this._db.put(MYEVENTPEER, JSON.stringify(toStore), (err) => {
            if (err) { return callback(err) }

            callback(null, this._server.address())
          })
        })
      })
    })
  })
}

HyperEmitter.prototype.stream = function (opts) {
  const result = duplexify.obj()
  const input = through2.obj((chunk, enc, next) => {
    this.emit(chunk.name, chunk.payload, next)
  })

  result.setWritable(input)

  this._hyperlog.ready(() => {
    const that = this
    const filter = through2.obj(function (change, enc, next) {
      const container = that.codecs.Event.decode(change.value)
      const name = container.name
      const decoder = that.codecs[name]
      let event = container.payload

      if (decoder) event = decoder.decode(event)

      this.push({
        name: name,
        payload: event
      })

      next()
    })

    const since = opts && opts.from === 'beginning' ? 0 : this._hyperlog.changes

    pump(this._hyperlog.createReadStream({
      since: since,
      live: true
    }), filter)

    result.setReadable(filter)
  })

  return result
}

HyperEmitter.prototype.close = function (callback) {
  const resources = [this._db]

  if (this.changeStream) resources.push(this.changeStream)
  if (this._listening) resources.push(this._server)

  Object.keys(this._peers).forEach((peerId) => {
    resources.unshift(this._peers[peerId])
  })

  this._closed = true
  this._parallel(this, destroyOrClose, resources, callback || noop)
}

export default HyperEmitter
