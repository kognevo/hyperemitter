// CommonJS wrapper for ESM module
// Provides CommonJS compatibility via dynamic import
const { createRequire } = require('node:module')
const { pathToFileURL } = require('node:url')
const path = require('node:path')
const { fileURLToPath } = require('node:url')

// For true CommonJS compatibility, we need to load the ESM module
// Since we can't use top-level await in .cjs, we'll export a getter
let HyperEmitterClass = null
let loadPromise = null

function loadHyperEmitter () {
  if (HyperEmitterClass) {
    return HyperEmitterClass
  }
  
  if (!loadPromise) {
    const esmPath = path.resolve(__dirname, 'hyperemitter.js')
    loadPromise = import(pathToFileURL(esmPath).href).then(mod => {
      HyperEmitterClass = mod.default
      return HyperEmitterClass
    })
  }
  
  // For synchronous access, we need to throw an error
  // Users should use ESM or dynamic import
  throw new Error(
    'HyperEmitter requires ESM. Use: import HyperEmitter from "hyperemitter" ' +
    'or const HyperEmitter = (await import("hyperemitter")).default'
  )
}

// Export a constructor function
function HyperEmitter (...args) {
  if (!HyperEmitterClass) {
    throw new Error(
      'HyperEmitter: Module not loaded synchronously. ' +
      'Please use ESM: import HyperEmitter from "hyperemitter" ' +
      'or use dynamic import in CommonJS'
    )
  }
  return new HyperEmitterClass(...args)
}

// Try to preload asynchronously
const esmPath = path.resolve(__dirname, 'hyperemitter.js')
import(pathToFileURL(esmPath).href)
  .then(mod => {
    HyperEmitterClass = mod.default
    // Copy prototype for instanceof checks
    if (HyperEmitterClass && HyperEmitterClass.prototype) {
      Object.setPrototypeOf(HyperEmitter, HyperEmitterClass)
      HyperEmitter.prototype = HyperEmitterClass.prototype
    }
  })
  .catch(() => {
    // Will error on first use
  })

module.exports = HyperEmitter
