// Single market runtime; no operator HTTP routes or public worker controls.
const fs = require('node:fs')
const path = require('node:path')
const python = path.join(__dirname, '.venv', 'Scripts', 'python.exe')
const policy = path.join(__dirname, 'visitor-policy.local.json')
for (const file of [python, policy, path.join(__dirname, 'config.local.json')]) {
  if (!fs.statSync(file).isFile()) throw new Error('Prepare the local runtime profile first')
}
module.exports = { apps: [{
  name: 'apollo-visitor', cwd: __dirname,
  script: path.join(__dirname, 'run_local.py'), interpreter: python,
  interpreter_args: ['-u', '-B'], args: ['--config', policy],
  exec_mode: 'fork', instances: 1, watch: false, autorestart: true,
  min_uptime: 60000, max_restarts: 5, restart_delay: 3000,
  stop_exit_codes: [0, 2], log_date_format: 'YYYY-MM-DD HH:mm:ss', windowsHide: true,
}] }
