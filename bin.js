#! /usr/bin/env node

import memdb from 'memdb'
import pump from 'pump'
import level from 'level'
import { readFileSync } from 'node:fs'
import { createScript, runInContext } from 'node:vm'
import minimist from 'minimist'
import { start } from 'node:repl'
import ndjson from 'ndjson'
import HyperEmitter from './hyperemitter.js'

const argv = minimist(process.argv.slice(2), {
  string: ['host', 'port', 'targetHost', 'db'],
  boolean: ['help', 'repl'],
  alias: {
    'targetHost': 'target-host',
    'targetPort': 'target-port',
    'fromScratch': 'from-scratch',
    'help': 'h'
  },
  default: {
    host: 'localhost',
    targetHost: 'localhost',
    fromScratch: false,
    repl: true
  }
})

function usage () {
  console.log('Usage: hypem <SCHEMA> [--schema SCHEMA] [--port PORT] [--host HOST]\n' +
              '                      [--target-host HOST] [--target-port PORT]\n' +
              '                      [--db PATH] [--no-repl] [--from-scratch]')
}

if (argv.help) {
  usage()
  process.exit(1)
}

let messages = null
if (argv._[0]) {
  messages = readFileSync(argv._[0])
} else if (argv.schema) {
  messages = readFileSync(argv.schema)
} else {
  console.error('Missing schema')
  console.log()
  usage()
  process.exit(1)
}

const db = argv.db ? level(argv.db) : memdb()
const hyper = new HyperEmitter(db, messages)
const startFn = argv.repl ? startREPL : startStream

if (argv.port) {
  hyper.listen(argv.port, argv.host, (err, bound) => {
    if (err) {
      throw err
    }

    if (argv.repl) {
      console.log('listening on', bound.port, bound.address)
    }

    connect(startFn)
  })
} else {
  connect(startFn)
}

function connect (next) {
  if (argv.targetHost && argv.targetPort) {
    hyper.connect(argv.targetPort, argv.targetHost, (err) => {
      if (err) {
        throw err
      }

      if (argv.repl) {
        console.log('connected to', argv.targetHost, argv.targetPort)
      }

      next()
    })
  } else {
    next()
  }
}

function startREPL (err) {
  if (err) {
    throw err
  }

  const instance = start({
    ignoreUndefined: true,
    eval: noOutputEval,
    input: process.stdin,
    output: process.stdout
  })

  instance.context.hyper = hyper

  Object.keys(hyper.messages).map((key) => {
    return hyper.messages[key]
  }).forEach((message) => {
    hyper.on(message.name, (msg) => {
      instance.inputStream.write('\n')
      console.log(message.name, msg)

      // undocumented function in node and io
      instance.displayPrompt()
    })
  })

  instance.on('exit', () => {
    process.exit(0)
  })
}

function noOutputEval (cmd, context, filename, callback) {
  let err

  if (cmd === '(\n)') {
    return callback(null, undefined)
  }

  let script
  try {
    script = createScript(cmd, {
      filename: filename,
      displayErrors: false
    })
  } catch (e) {
    console.log('parse error', e)
    err = e
  }

  if (!err) {
    try {
      script.runInContext(context, { displayErrors: false })
    } catch (e) {
      err = e
    }
  }

  callback(err, undefined)
}

function startStream () {
  const opts = argv.fromScratch ? { from: 'beginning' } : null
  const stream = hyper.stream(opts)

  // input pipeline
  pump(
    process.stdin,
    ndjson.parse(),
    stream
  )

  // output pipeline
  pump(
    stream,
    ndjson.serialize(),
    process.stdout
  )
}
