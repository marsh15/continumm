'use strict';

const fs = require('node:fs');
const path = require('node:path');

const WORKSPACE = '/vercel/sandbox/workspace';
const MAX_INPUT_BYTES = 65536;
const MAX_CHUNK_CHARS = 4096;
const MAX_CHUNKS = 32;
const SAFE_PATH = /^[A-Za-z0-9_./-]+$/;

class ProbeFailure extends Error {
  constructor(code) {
    super(code);
    this.code = code;
  }
}

function parseArgs(argv) {
  if (argv.length < 4 || argv[0] !== '--entry' || argv[2] !== '--inputs-b64') {
    throw new ProbeFailure('probe_arguments_invalid');
  }
  const entry = argv[1];
  const chunks = argv.slice(3);
  if (chunks.length < 1 || chunks.length > MAX_CHUNKS || chunks.some((part) => !part || part.length > MAX_CHUNK_CHARS)) {
    throw new ProbeFailure('probe_arguments_invalid');
  }
  if (!entry || entry.includes('\0') || entry.includes('\\') || entry.startsWith('/') ||
      !SAFE_PATH.test(entry) || entry.split('/').some((part) => !part || part === '.' || part === '..') ||
      !entry.endsWith('.cjs')) {
    throw new ProbeFailure('probe_entry_invalid');
  }
  let raw;
  let inputs;
  try {
    const encoded = chunks.join('');
    if (!/^[A-Za-z0-9+/]*={0,2}$/.test(encoded)) throw new Error();
    raw = Buffer.from(encoded, 'base64');
    if (raw.toString('base64') !== encoded || raw.length > MAX_INPUT_BYTES) {
      throw new ProbeFailure(raw.length > MAX_INPUT_BYTES ? 'probe_inputs_size_limit' : 'probe_inputs_invalid');
    }
    inputs = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(raw));
  } catch (error) {
    if (error instanceof ProbeFailure) throw error;
    throw new ProbeFailure('probe_inputs_invalid');
  }
  if (!Array.isArray(inputs) || inputs.length < 1 || inputs.length > 32) {
    throw new ProbeFailure('probe_inputs_invalid');
  }
  return { entry, inputs };
}

function resolveEntry(workspace, entry) {
  let root;
  let current;
  try {
    root = fs.realpathSync(workspace);
    current = root;
    for (const part of entry.split('/')) {
      current = path.join(current, part);
      if (fs.lstatSync(current).isSymbolicLink()) throw new ProbeFailure('probe_entry_invalid');
    }
    const resolved = fs.realpathSync(current);
    if (!resolved.startsWith(`${root}${path.sep}`) || !fs.statSync(resolved).isFile() || !resolved.endsWith('.cjs')) {
      throw new ProbeFailure('probe_entry_invalid');
    }
    return resolved;
  } catch (error) {
    if (error instanceof ProbeFailure) throw error;
    throw new ProbeFailure('probe_entry_invalid');
  }
}

function validateJson(value, active = new Set(), depth = 0) {
  if (depth > 16) throw new ProbeFailure('probe_result_invalid');
  if (value === null || typeof value === 'boolean' || typeof value === 'string') return;
  if (typeof value === 'number') {
    if (!Number.isFinite(value) || (Number.isInteger(value) && Math.abs(value) > Number.MAX_SAFE_INTEGER)) {
      throw new ProbeFailure('probe_result_invalid');
    }
    return;
  }
  if (typeof value !== 'object' || typeof value === 'undefined') {
    throw new ProbeFailure('probe_result_invalid');
  }
  if (active.has(value)) throw new ProbeFailure('probe_result_invalid');
  const proto = Object.getPrototypeOf(value);
  if (Array.isArray(value) ? proto !== Array.prototype : (proto !== Object.prototype && proto !== null)) {
    throw new ProbeFailure('probe_result_invalid');
  }
  if (Object.getOwnPropertySymbols(value).length !== 0) throw new ProbeFailure('probe_result_invalid');
  active.add(value);
  try {
    if (Array.isArray(value)) {
      for (let index = 0; index < value.length; index += 1) {
        if (!Object.prototype.hasOwnProperty.call(value, index)) throw new ProbeFailure('probe_result_invalid');
        validateJson(value[index], active, depth + 1);
      }
    } else {
      for (const [key, descriptor] of Object.entries(Object.getOwnPropertyDescriptors(value))) {
        if (!Object.prototype.hasOwnProperty.call(descriptor, 'value')) throw new ProbeFailure('probe_result_invalid');
        validateJson(key, active, depth + 1);
        validateJson(descriptor.value, active, depth + 1);
      }
    }
  } finally {
    active.delete(value);
  }
}

function run(argv = process.argv.slice(2), workspace = WORKSPACE) {
  try {
    const { entry, inputs } = parseArgs(argv);
    const entryFile = resolveEntry(workspace, entry);
    const captured = { write() { return true; } };
    const originalStdout = process.stdout.write;
    const originalStderr = process.stderr.write;
    let values;
    try {
      process.stdout.write = captured.write;
      process.stderr.write = captured.write;
      const entryModule = require(entryFile);
      if (!entryModule || typeof entryModule.solve !== 'function') throw new ProbeFailure('probe_entry_invalid');
      values = inputs.map((input) => entryModule.solve(input));
    } finally {
      process.stdout.write = originalStdout;
      process.stderr.write = originalStderr;
    }
    validateJson(values);
    const output = JSON.stringify({ protocol_version: 1, values });
    if (Buffer.byteLength(output, 'utf8') > MAX_INPUT_BYTES) throw new ProbeFailure('probe_result_size_limit');
    process.stdout.write(output);
    return 0;
  } catch (error) {
    const code = error instanceof ProbeFailure ? error.code : 'probe_execution_failed';
    process.stderr.write(`${code}\n`);
    return 2;
  }
}

module.exports = { run };
if (require.main === module) process.exitCode = run();
