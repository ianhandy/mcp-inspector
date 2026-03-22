/* ═══════════════════════════════════════════════════════════════
   MCP Inspector — Universal MCP Server Explorer
   Connects to any MCP server via Streamable HTTP or SSE transport,
   renders schemas as interactive forms, and lets you invoke tools.
   ═══════════════════════════════════════════════════════════════ */

// ── Particle Background ──
(function initParticles() {
  const canvas = document.getElementById('particles');
  const ctx = canvas.getContext('2d');
  let particles = [];
  const PARTICLE_COUNT = 40;

  function resize() {
    canvas.width = window.innerWidth;
    canvas.height = window.innerHeight;
  }
  resize();
  window.addEventListener('resize', resize);

  for (let i = 0; i < PARTICLE_COUNT; i++) {
    particles.push({
      x: Math.random() * canvas.width,
      y: Math.random() * canvas.height,
      vx: (Math.random() - 0.5) * 0.3,
      vy: (Math.random() - 0.5) * 0.3,
      r: Math.random() * 2 + 0.5,
      a: Math.random() * 0.3 + 0.05,
    });
  }

  function draw() {
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    for (const p of particles) {
      p.x += p.vx;
      p.y += p.vy;
      if (p.x < 0) p.x = canvas.width;
      if (p.x > canvas.width) p.x = 0;
      if (p.y < 0) p.y = canvas.height;
      if (p.y > canvas.height) p.y = 0;
      ctx.beginPath();
      ctx.arc(p.x, p.y, p.r, 0, Math.PI * 2);
      ctx.fillStyle = `rgba(221,193,101,${p.a})`;
      ctx.fill();
    }
    requestAnimationFrame(draw);
  }
  draw();
})();


// ── MCP Client ──
class MCPClient {
  constructor() {
    this.url = '';
    this.transport = 'streamable-http';
    this.headers = {};
    this.sessionId = null;
    this.serverInfo = null;
    this.capabilities = null;
    this.requestId = 0;
    this.sseSource = null;
    this.pendingRequests = new Map();
  }

  async connect(url, transport, headers = {}) {
    this.url = url;
    this.transport = transport;
    this.headers = headers;
    this.requestId = 0;
    this.sessionId = null;

    // Initialize connection
    const initResult = await this.sendRequest('initialize', {
      protocolVersion: '2025-03-26',
      capabilities: {},
      clientInfo: { name: 'MCP Inspector', version: '1.0.0' }
    });

    this.serverInfo = initResult.serverInfo || {};
    this.capabilities = initResult.capabilities || {};

    // Send initialized notification
    await this.sendNotification('notifications/initialized', {});

    return initResult;
  }

  async sendRequest(method, params = {}) {
    this.requestId++;
    const id = this.requestId;
    const body = {
      jsonrpc: '2.0',
      id,
      method,
      params
    };

    if (this.transport === 'streamable-http') {
      return this._sendStreamableHTTP(body);
    } else {
      return this._sendSSE(body);
    }
  }

  async sendNotification(method, params = {}) {
    const body = {
      jsonrpc: '2.0',
      method,
      params
    };

    try {
      const hdrs = {
        'Content-Type': 'application/json',
        ...this.headers
      };
      if (this.sessionId) hdrs['mcp-session-id'] = this.sessionId;

      await fetch(this.url, {
        method: 'POST',
        headers: hdrs,
        body: JSON.stringify(body)
      });
    } catch (e) {
      // Notifications don't require responses
    }
  }

  async _sendStreamableHTTP(body) {
    const hdrs = {
      'Content-Type': 'application/json',
      'Accept': 'application/json, text/event-stream',
      ...this.headers
    };
    if (this.sessionId) hdrs['mcp-session-id'] = this.sessionId;

    const response = await fetch(this.url, {
      method: 'POST',
      headers: hdrs,
      body: JSON.stringify(body)
    });

    if (!response.ok) {
      const text = await response.text();
      throw new Error(`HTTP ${response.status}: ${text}`);
    }

    // Capture session ID
    const sid = response.headers.get('mcp-session-id');
    if (sid) this.sessionId = sid;

    const contentType = response.headers.get('content-type') || '';

    if (contentType.includes('text/event-stream')) {
      // SSE response — parse events
      return this._parseSSEResponse(response, body.id);
    } else {
      // JSON response
      const result = await response.json();
      if (result.error) throw new Error(result.error.message || JSON.stringify(result.error));
      return result.result;
    }
  }

  async _parseSSEResponse(response, requestId) {
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    let result = null;

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });

      const lines = buffer.split('\n');
      buffer = lines.pop() || '';

      let currentEvent = '';
      let currentData = '';
      for (const line of lines) {
        if (line.startsWith('event:')) {
          currentEvent = line.slice(6).trim();
        } else if (line.startsWith('data:')) {
          currentData += line.slice(5).trim();
        } else if (line === '') {
          if (currentData) {
            try {
              const parsed = JSON.parse(currentData);
              if (parsed.id === requestId) {
                if (parsed.error) throw new Error(parsed.error.message || JSON.stringify(parsed.error));
                result = parsed.result;
              }
            } catch (e) {
              if (e.message && !e.message.startsWith('Unexpected')) throw e;
            }
          }
          currentEvent = '';
          currentData = '';
        }
      }
    }

    if (!result) throw new Error('No response received from SSE stream');
    return result;
  }

  async _sendSSE(body) {
    // Legacy SSE transport: POST to the endpoint, read SSE response
    // First, establish SSE connection if not already done
    if (!this.sseSource) {
      await this._establishSSE();
    }

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pendingRequests.delete(body.id);
        reject(new Error('Request timeout (30s)'));
      }, 30000);

      this.pendingRequests.set(body.id, { resolve, reject, timeout });

      const hdrs = {
        'Content-Type': 'application/json',
        ...this.headers
      };

      fetch(this._ssePostUrl || this.url, {
        method: 'POST',
        headers: hdrs,
        body: JSON.stringify(body)
      }).catch(err => {
        clearTimeout(timeout);
        this.pendingRequests.delete(body.id);
        reject(err);
      });
    });
  }

  async _establishSSE() {
    return new Promise((resolve, reject) => {
      const sseUrl = this.url.replace(/\/?$/, '') + (this.url.includes('?') ? '&' : '');
      this.sseSource = new EventSource(sseUrl);

      this.sseSource.addEventListener('endpoint', (event) => {
        // The server sends the POST endpoint for sending messages
        const data = JSON.parse(event.data);
        if (data.url) {
          // Resolve relative URL against base
          try {
            this._ssePostUrl = new URL(data.url, this.url).href;
          } catch {
            this._ssePostUrl = data.url;
          }
        }
        resolve();
      });

      this.sseSource.addEventListener('message', (event) => {
        try {
          const parsed = JSON.parse(event.data);
          if (parsed.id && this.pendingRequests.has(parsed.id)) {
            const { resolve: res, reject: rej, timeout } = this.pendingRequests.get(parsed.id);
            clearTimeout(timeout);
            this.pendingRequests.delete(parsed.id);
            if (parsed.error) {
              rej(new Error(parsed.error.message || JSON.stringify(parsed.error)));
            } else {
              res(parsed.result);
            }
          }
        } catch (e) { /* ignore parse errors */ }
      });

      this.sseSource.onerror = () => {
        reject(new Error('SSE connection failed'));
      };

      // Timeout for initial connection
      setTimeout(() => reject(new Error('SSE connection timeout')), 10000);
    });
  }

  disconnect() {
    if (this.sseSource) {
      this.sseSource.close();
      this.sseSource = null;
    }
    // Send terminate if using streamable HTTP
    if (this.sessionId && this.transport === 'streamable-http') {
      const hdrs = { ...this.headers };
      if (this.sessionId) hdrs['mcp-session-id'] = this.sessionId;
      fetch(this.url, { method: 'DELETE', headers: hdrs }).catch(() => {});
    }
    this.sessionId = null;
    this.serverInfo = null;
    this.capabilities = null;
    for (const [, { reject, timeout }] of this.pendingRequests) {
      clearTimeout(timeout);
      reject(new Error('Disconnected'));
    }
    this.pendingRequests.clear();
  }
}


// ── Application ──
class App {
  constructor() {
    this.client = new MCPClient();
    this.connected = false;
    this.tools = [];
    this.resources = [];
    this.prompts = [];
    this.history = [];
    this.currentTab = 'tools';
    this.savedServers = JSON.parse(localStorage.getItem('mcp-inspector-servers') || '[]');
    this.renderSavedServers();
  }

  // ── Connection ──
  async connect() {
    const url = document.getElementById('serverUrl').value.trim();
    if (!url) return this.toast('Enter a server URL', 'error');

    const transport = document.getElementById('transportType').value;
    let headers = {};
    const headersText = document.getElementById('customHeaders').value.trim();
    if (headersText) {
      try {
        headers = JSON.parse(headersText);
      } catch {
        return this.toast('Invalid headers JSON', 'error');
      }
    }

    this.setConnectionStatus('connecting', 'Connecting...');
    const btn = document.getElementById('connectBtn');
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span> Connecting...';

    try {
      const result = await this.client.connect(url, transport, headers);
      this.connected = true;
      const name = this.client.serverInfo?.name || 'MCP Server';
      this.setConnectionStatus('connected', `Connected to ${name}`);
      this.showServerInfo(result);
      this.saveServer(url, transport, headers, name);

      // Fetch capabilities
      await this.fetchAll();

      // Show connected state
      document.getElementById('welcomeState').style.display = 'none';
      document.getElementById('connectedState').style.display = 'block';

      this.toast(`Connected to ${name}`, 'success');
    } catch (err) {
      this.setConnectionStatus('disconnected', 'Connection failed');
      this.toast(`Failed: ${err.message}`, 'error');
    }

    btn.disabled = false;
    btn.innerHTML = '<span class="btn-icon">⚡</span> Connect';
  }

  disconnect() {
    this.client.disconnect();
    this.connected = false;
    this.tools = [];
    this.resources = [];
    this.prompts = [];
    this.setConnectionStatus('disconnected', 'No server connected');
    document.getElementById('welcomeState').style.display = '';
    document.getElementById('connectedState').style.display = 'none';
    document.getElementById('serverInfo').style.display = 'none';
    this.toast('Disconnected', 'info');
  }

  async fetchAll() {
    const promises = [];

    if (!this.client.capabilities || this.client.capabilities.tools) {
      promises.push(this.fetchTools());
    }
    if (!this.client.capabilities || this.client.capabilities.resources) {
      promises.push(this.fetchResources());
    }
    if (!this.client.capabilities || this.client.capabilities.prompts) {
      promises.push(this.fetchPrompts());
    }

    await Promise.allSettled(promises);
  }

  async fetchTools() {
    try {
      const result = await this.client.sendRequest('tools/list');
      this.tools = result.tools || [];
      document.getElementById('toolsCount').textContent = this.tools.length;
      this.renderTools();
    } catch (e) {
      console.warn('tools/list failed:', e);
    }
  }

  async fetchResources() {
    try {
      const result = await this.client.sendRequest('resources/list');
      this.resources = result.resources || [];
      document.getElementById('resourcesCount').textContent = this.resources.length;
      this.renderResources();
    } catch (e) {
      console.warn('resources/list failed:', e);
    }
  }

  async fetchPrompts() {
    try {
      const result = await this.client.sendRequest('prompts/list');
      this.prompts = result.prompts || [];
      document.getElementById('promptsCount').textContent = this.prompts.length;
      this.renderPrompts();
    } catch (e) {
      console.warn('prompts/list failed:', e);
    }
  }

  // ── Server Info ──
  showServerInfo(initResult) {
    const section = document.getElementById('serverInfo');
    const content = document.getElementById('serverInfoContent');
    const si = this.client.serverInfo || {};
    const caps = this.client.capabilities || {};

    let capsHtml = '';
    for (const [key, val] of Object.entries(caps)) {
      if (val) capsHtml += `<span class="info-badge">${key}</span>`;
    }

    content.innerHTML = `
      <div class="info-row"><span class="info-label">Name</span><span class="info-value">${esc(si.name || 'Unknown')}</span></div>
      <div class="info-row"><span class="info-label">Version</span><span class="info-value">${esc(si.version || '—')}</span></div>
      <div class="info-row"><span class="info-label">Protocol</span><span class="info-value">${esc(initResult.protocolVersion || '—')}</span></div>
      ${capsHtml ? `<div class="info-row"><span class="info-label">Capabilities</span><span class="info-value">${capsHtml}</span></div>` : ''}
      <button class="btn btn-danger btn-sm disconnect-btn" onclick="app.disconnect()">Disconnect</button>
    `;
    section.style.display = '';
  }

  // ── Saved Servers ──
  saveServer(url, transport, headers, name) {
    const existing = this.savedServers.findIndex(s => s.url === url);
    const entry = { url, transport, headers, name, lastUsed: Date.now() };
    if (existing >= 0) {
      this.savedServers[existing] = entry;
    } else {
      this.savedServers.unshift(entry);
    }
    this.savedServers = this.savedServers.slice(0, 20);
    localStorage.setItem('mcp-inspector-servers', JSON.stringify(this.savedServers));
    this.renderSavedServers();
  }

  removeSavedServer(index) {
    this.savedServers.splice(index, 1);
    localStorage.setItem('mcp-inspector-servers', JSON.stringify(this.savedServers));
    this.renderSavedServers();
  }

  loadSavedServer(index) {
    const s = this.savedServers[index];
    if (!s) return;
    document.getElementById('serverUrl').value = s.url;
    document.getElementById('transportType').value = s.transport;
    document.getElementById('customHeaders').value = Object.keys(s.headers || {}).length ? JSON.stringify(s.headers, null, 2) : '';
  }

  renderSavedServers() {
    const list = document.getElementById('savedServersList');
    if (!this.savedServers.length) {
      list.innerHTML = '<p class="text-dim text-sm">No saved servers yet</p>';
      return;
    }
    list.innerHTML = this.savedServers.map((s, i) => `
      <div class="saved-server" onclick="app.loadSavedServer(${i})">
        <div>
          <div class="saved-server-name">${esc(s.name || 'Server')}</div>
          <div class="saved-server-url">${esc(s.url)}</div>
        </div>
        <button class="saved-server-delete" onclick="event.stopPropagation();app.removeSavedServer(${i})" title="Remove">&times;</button>
      </div>
    `).join('');
  }

  // ── Tabs ──
  switchTab(tab) {
    this.currentTab = tab;
    document.querySelectorAll('.tab').forEach(t => t.classList.toggle('active', t.dataset.tab === tab));
    document.querySelectorAll('.tab-panel').forEach(p => p.classList.toggle('active', p.id === `panel-${tab}`));
  }

  // ── Tools ──
  renderTools(filter = '') {
    const list = document.getElementById('toolsList');
    const filtered = this.tools.filter(t =>
      !filter || t.name.toLowerCase().includes(filter) || (t.description || '').toLowerCase().includes(filter)
    );

    if (!filtered.length) {
      list.innerHTML = `<div class="no-results">${this.tools.length ? 'No tools match your search' : 'This server has no tools'}</div>`;
      return;
    }

    list.innerHTML = filtered.map((tool, i) => {
      const schema = tool.inputSchema || {};
      const props = schema.properties || {};
      const required = schema.required || [];
      const paramCount = Object.keys(props).length;
      const reqCount = required.length;

      return `
        <div class="tool-card" onclick="app.openToolModal('${esc(tool.name)}')">
          <div class="card-header">
            <div>
              <div class="card-name">${esc(tool.name)}</div>
              ${tool.description ? `<div class="card-description">${esc(tool.description)}</div>` : ''}
            </div>
            <button class="card-action" onclick="event.stopPropagation();app.openToolModal('${esc(tool.name)}')">Execute</button>
          </div>
          <div class="card-meta">
            ${paramCount ? `<span class="meta-tag">${paramCount} param${paramCount > 1 ? 's' : ''}</span>` : '<span class="meta-tag">no params</span>'}
            ${reqCount ? `<span class="meta-tag required">${reqCount} required</span>` : ''}
          </div>
        </div>
      `;
    }).join('');
  }

  filterTools() {
    this.renderTools(document.getElementById('toolSearch').value.toLowerCase().trim());
  }

  openToolModal(toolName) {
    const tool = this.tools.find(t => t.name === toolName);
    if (!tool) return;

    const modal = document.getElementById('modalOverlay');
    document.getElementById('modalTitle').textContent = `Execute: ${tool.name}`;

    const body = document.getElementById('modalBody');
    const schema = tool.inputSchema || {};
    const props = schema.properties || {};
    const required = schema.required || [];

    let formHtml = '';
    if (tool.description) {
      formHtml += `<p class="card-description" style="margin-bottom:16px">${esc(tool.description)}</p>`;
    }

    const propEntries = Object.entries(props);
    if (propEntries.length === 0) {
      formHtml += '<p class="text-dim">This tool takes no parameters.</p>';
    } else {
      for (const [name, prop] of propEntries) {
        formHtml += this.renderFormField(name, prop, required.includes(name));
      }
    }

    // Raw JSON toggle
    formHtml += `
      <div style="margin-top:10px">
        <button class="form-raw-toggle" onclick="app.toggleRawJson()">Switch to raw JSON</button>
      </div>
      <div id="rawJsonEditor" style="display:none;margin-top:10px">
        <textarea id="rawJsonInput" class="input input-textarea" rows="8" placeholder='{"param": "value"}'>${propEntries.length ? JSON.stringify(Object.fromEntries(propEntries.map(([k, v]) => [k, v.default !== undefined ? v.default : ''])), null, 2) : '{}'}</textarea>
      </div>
    `;

    formHtml += `
      <div class="form-actions">
        <button class="btn btn-primary" id="executeBtn" onclick="app.executeTool('${esc(toolName)}')">
          <span class="btn-icon">⚡</span> Execute
        </button>
        <button class="btn btn-secondary" onclick="app.closeModal()">Cancel</button>
      </div>
      <div id="toolResult"></div>
    `;

    body.innerHTML = formHtml;
    modal.style.display = '';
    this._rawJsonMode = false;
  }

  renderFormField(name, prop, isRequired) {
    const type = prop.type || 'string';
    const desc = prop.description || '';
    const reqMark = isRequired ? '<span class="field-required">*</span>' : '';
    const typeLabel = `<span class="field-type">${type}${prop.enum ? ' (enum)' : ''}</span>`;

    let input = '';
    const inputId = `field-${name}`;

    if (prop.enum) {
      input = `<select id="${inputId}" class="input">
        <option value="">— select —</option>
        ${prop.enum.map(v => `<option value="${esc(String(v))}">${esc(String(v))}</option>`).join('')}
      </select>`;
    } else if (type === 'boolean') {
      input = `<label class="checkbox-label"><input type="checkbox" id="${inputId}" ${prop.default ? 'checked' : ''}> ${esc(name)}</label>`;
    } else if (type === 'number' || type === 'integer') {
      input = `<input type="number" id="${inputId}" class="input" placeholder="${esc(String(prop.default ?? ''))}" ${prop.minimum !== undefined ? `min="${prop.minimum}"` : ''} ${prop.maximum !== undefined ? `max="${prop.maximum}"` : ''}>`;
    } else if (type === 'object' || type === 'array') {
      const defaultVal = prop.default ? JSON.stringify(prop.default, null, 2) : '';
      input = `<textarea id="${inputId}" class="input input-textarea" rows="3" placeholder='${type === 'array' ? '["item1", "item2"]' : '{"key": "value"}'}'>${esc(defaultVal)}</textarea>`;
    } else {
      // string
      if (desc.toLowerCase().includes('multiline') || desc.toLowerCase().includes('content') || desc.toLowerCase().includes('body') || desc.toLowerCase().includes('code')) {
        input = `<textarea id="${inputId}" class="input input-textarea" rows="3" placeholder="${esc(String(prop.default ?? ''))}">${esc(String(prop.default ?? ''))}</textarea>`;
      } else {
        input = `<input type="text" id="${inputId}" class="input" placeholder="${esc(String(prop.default ?? ''))}" value="${esc(String(prop.default ?? ''))}">`;
      }
    }

    return `
      <div class="form-group" data-field="${esc(name)}" data-type="${type}">
        <label>${esc(name)}${reqMark}${typeLabel}</label>
        ${desc ? `<div class="field-description">${esc(desc)}</div>` : ''}
        ${input}
      </div>
    `;
  }

  toggleRawJson() {
    this._rawJsonMode = !this._rawJsonMode;
    const editor = document.getElementById('rawJsonEditor');
    const formGroups = document.querySelectorAll('.modal .form-group');
    const toggle = document.querySelector('.form-raw-toggle');

    if (this._rawJsonMode) {
      // Collect current form values into JSON
      const args = this.collectFormArgs();
      document.getElementById('rawJsonInput').value = JSON.stringify(args, null, 2);
      editor.style.display = '';
      formGroups.forEach(g => g.style.display = 'none');
      toggle.textContent = 'Switch to form view';
    } else {
      editor.style.display = 'none';
      formGroups.forEach(g => g.style.display = '');
      toggle.textContent = 'Switch to raw JSON';
    }
  }

  collectFormArgs() {
    if (this._rawJsonMode) {
      try {
        return JSON.parse(document.getElementById('rawJsonInput').value);
      } catch {
        return {};
      }
    }

    const args = {};
    const groups = document.querySelectorAll('.modal .form-group');
    for (const group of groups) {
      const name = group.dataset.field;
      const type = group.dataset.type;
      const input = group.querySelector('input, textarea, select');
      if (!input) continue;

      let value;
      if (type === 'boolean') {
        value = input.checked;
      } else if (type === 'number' || type === 'integer') {
        if (input.value === '') continue;
        value = Number(input.value);
      } else if (type === 'object' || type === 'array') {
        if (!input.value.trim()) continue;
        try {
          value = JSON.parse(input.value);
        } catch {
          value = input.value;
        }
      } else {
        if (input.value === '') continue;
        value = input.value;
      }

      args[name] = value;
    }
    return args;
  }

  async executeTool(toolName) {
    const args = this.collectFormArgs();
    const btn = document.getElementById('executeBtn');
    const resultDiv = document.getElementById('toolResult');
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span> Executing...';
    resultDiv.innerHTML = '<div class="loading"><span class="spinner"></span> Waiting for response...</div>';

    const startTime = performance.now();

    try {
      const result = await this.client.sendRequest('tools/call', { name: toolName, arguments: args });
      const elapsed = Math.round(performance.now() - startTime);
      this.addHistory('tools/call', { name: toolName, arguments: args }, result, true, elapsed);

      resultDiv.innerHTML = `
        <div class="result-container">
          <div class="result-header">Response <span class="result-time">${elapsed}ms</span></div>
          <div class="json-view">${this.renderMCPContent(result)}</div>
        </div>
      `;
    } catch (err) {
      const elapsed = Math.round(performance.now() - startTime);
      this.addHistory('tools/call', { name: toolName, arguments: args }, { error: err.message }, false, elapsed);
      resultDiv.innerHTML = `
        <div class="result-container">
          <div class="result-header">Error <span class="result-time">${elapsed}ms</span></div>
          <div class="result-error">${esc(err.message)}</div>
        </div>
      `;
    }

    btn.disabled = false;
    btn.innerHTML = '<span class="btn-icon">⚡</span> Execute';
  }

  renderMCPContent(result) {
    // MCP tool results contain a "content" array with type/text entries
    if (result && Array.isArray(result.content)) {
      return result.content.map(c => {
        if (c.type === 'text') {
          // Try to parse as JSON for pretty display
          try {
            const parsed = JSON.parse(c.text);
            return syntaxHighlight(JSON.stringify(parsed, null, 2));
          } catch {
            return esc(c.text);
          }
        } else if (c.type === 'image') {
          return `<img src="data:${c.mimeType};base64,${c.data}" style="max-width:100%;border-radius:6px;margin:8px 0">`;
        } else if (c.type === 'resource') {
          return `<div><strong>Resource:</strong> ${esc(c.resource?.uri || '')}</div>\n${esc(c.resource?.text || '')}`;
        }
        return syntaxHighlight(JSON.stringify(c, null, 2));
      }).join('\n\n');
    }
    return syntaxHighlight(JSON.stringify(result, null, 2));
  }

  // ── Resources ──
  renderResources(filter = '') {
    const list = document.getElementById('resourcesList');
    const filtered = this.resources.filter(r =>
      !filter || r.name.toLowerCase().includes(filter) || (r.uri || '').toLowerCase().includes(filter)
    );

    if (!filtered.length) {
      list.innerHTML = `<div class="no-results">${this.resources.length ? 'No resources match your search' : 'This server has no resources'}</div>`;
      return;
    }

    list.innerHTML = filtered.map(r => `
      <div class="resource-card" onclick="app.readResource('${esc(r.uri)}')">
        <div class="card-header">
          <div>
            <div class="card-name">${esc(r.name || r.uri)}</div>
            <div class="resource-uri">${esc(r.uri)}</div>
            ${r.description ? `<div class="card-description">${esc(r.description)}</div>` : ''}
          </div>
          <button class="card-action" onclick="event.stopPropagation();app.readResource('${esc(r.uri)}')">Read</button>
        </div>
        <div class="card-meta">
          ${r.mimeType ? `<span class="meta-tag">${esc(r.mimeType)}</span>` : ''}
        </div>
      </div>
    `).join('');
  }

  filterResources() {
    this.renderResources(document.getElementById('resourceSearch').value.toLowerCase().trim());
  }

  async readResource(uri) {
    const modal = document.getElementById('modalOverlay');
    document.getElementById('modalTitle').textContent = `Resource: ${uri}`;
    const body = document.getElementById('modalBody');
    body.innerHTML = '<div class="loading"><span class="spinner"></span> Reading resource...</div>';
    modal.style.display = '';

    const startTime = performance.now();
    try {
      const result = await this.client.sendRequest('resources/read', { uri });
      const elapsed = Math.round(performance.now() - startTime);
      this.addHistory('resources/read', { uri }, result, true, elapsed);

      let contentHtml = '';
      if (result.contents && result.contents.length) {
        for (const c of result.contents) {
          if (c.text) {
            try {
              const parsed = JSON.parse(c.text);
              contentHtml += `<div class="json-view">${syntaxHighlight(JSON.stringify(parsed, null, 2))}</div>`;
            } catch {
              contentHtml += `<div class="json-view">${esc(c.text)}</div>`;
            }
          } else if (c.blob) {
            contentHtml += `<div class="text-dim">Binary content (${c.mimeType || 'unknown type'})</div>`;
          }
        }
      } else {
        contentHtml = `<div class="json-view">${syntaxHighlight(JSON.stringify(result, null, 2))}</div>`;
      }

      body.innerHTML = `
        <div class="result-header">Content <span class="result-time">${elapsed}ms</span></div>
        ${contentHtml}
      `;
    } catch (err) {
      const elapsed = Math.round(performance.now() - startTime);
      this.addHistory('resources/read', { uri }, { error: err.message }, false, elapsed);
      body.innerHTML = `<div class="result-error">${esc(err.message)}</div>`;
    }
  }

  // ── Prompts ──
  renderPrompts(filter = '') {
    const list = document.getElementById('promptsList');
    const filtered = this.prompts.filter(p =>
      !filter || p.name.toLowerCase().includes(filter) || (p.description || '').toLowerCase().includes(filter)
    );

    if (!filtered.length) {
      list.innerHTML = `<div class="no-results">${this.prompts.length ? 'No prompts match your search' : 'This server has no prompts'}</div>`;
      return;
    }

    list.innerHTML = filtered.map(p => {
      const args = p.arguments || [];
      return `
        <div class="prompt-card" onclick="app.openPromptModal('${esc(p.name)}')">
          <div class="card-header">
            <div>
              <div class="card-name">${esc(p.name)}</div>
              ${p.description ? `<div class="card-description">${esc(p.description)}</div>` : ''}
            </div>
            <button class="card-action" onclick="event.stopPropagation();app.openPromptModal('${esc(p.name)}')">Get</button>
          </div>
          <div class="card-meta">
            ${args.length ? `<span class="meta-tag">${args.length} argument${args.length > 1 ? 's' : ''}</span>` : '<span class="meta-tag">no arguments</span>'}
          </div>
        </div>
      `;
    }).join('');
  }

  filterPrompts() {
    this.renderPrompts(document.getElementById('promptSearch').value.toLowerCase().trim());
  }

  openPromptModal(promptName) {
    const prompt = this.prompts.find(p => p.name === promptName);
    if (!prompt) return;

    const modal = document.getElementById('modalOverlay');
    document.getElementById('modalTitle').textContent = `Prompt: ${prompt.name}`;
    const body = document.getElementById('modalBody');

    const args = prompt.arguments || [];
    let formHtml = '';
    if (prompt.description) {
      formHtml += `<p class="card-description" style="margin-bottom:16px">${esc(prompt.description)}</p>`;
    }

    if (args.length === 0) {
      formHtml += '<p class="text-dim">This prompt takes no arguments.</p>';
    } else {
      for (const arg of args) {
        formHtml += `
          <div class="form-group" data-field="${esc(arg.name)}" data-type="string">
            <label>${esc(arg.name)}${arg.required ? '<span class="field-required">*</span>' : ''}</label>
            ${arg.description ? `<div class="field-description">${esc(arg.description)}</div>` : ''}
            <input type="text" id="field-${esc(arg.name)}" class="input" placeholder="">
          </div>
        `;
      }
    }

    formHtml += `
      <div class="form-actions">
        <button class="btn btn-primary" id="executeBtn" onclick="app.getPrompt('${esc(promptName)}')">
          <span class="btn-icon">💬</span> Get Prompt
        </button>
        <button class="btn btn-secondary" onclick="app.closeModal()">Cancel</button>
      </div>
      <div id="toolResult"></div>
    `;

    body.innerHTML = formHtml;
    modal.style.display = '';
  }

  async getPrompt(promptName) {
    const prompt = this.prompts.find(p => p.name === promptName);
    const args = {};
    const groups = document.querySelectorAll('.modal .form-group');
    for (const group of groups) {
      const name = group.dataset.field;
      const input = group.querySelector('input');
      if (input && input.value) args[name] = input.value;
    }

    const btn = document.getElementById('executeBtn');
    const resultDiv = document.getElementById('toolResult');
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span> Loading...';
    resultDiv.innerHTML = '<div class="loading"><span class="spinner"></span> Fetching prompt...</div>';

    const startTime = performance.now();
    try {
      const result = await this.client.sendRequest('prompts/get', { name: promptName, arguments: args });
      const elapsed = Math.round(performance.now() - startTime);
      this.addHistory('prompts/get', { name: promptName, arguments: args }, result, true, elapsed);

      let messagesHtml = '';
      if (result.messages) {
        messagesHtml = result.messages.map(m => `
          <div style="margin-bottom:10px">
            <div style="font-size:0.72rem;color:var(--text-dim);text-transform:uppercase;margin-bottom:4px">${esc(m.role)}</div>
            <div class="json-view">${m.content?.type === 'text' ? esc(m.content.text) : syntaxHighlight(JSON.stringify(m.content, null, 2))}</div>
          </div>
        `).join('');
      }

      resultDiv.innerHTML = `
        <div class="result-container">
          <div class="result-header">Prompt Messages <span class="result-time">${elapsed}ms</span></div>
          ${messagesHtml || `<div class="json-view">${syntaxHighlight(JSON.stringify(result, null, 2))}</div>`}
        </div>
      `;
    } catch (err) {
      const elapsed = Math.round(performance.now() - startTime);
      this.addHistory('prompts/get', { name: promptName, arguments: args }, { error: err.message }, false, elapsed);
      resultDiv.innerHTML = `<div class="result-error">${esc(err.message)}</div>`;
    }

    btn.disabled = false;
    btn.innerHTML = '<span class="btn-icon">💬</span> Get Prompt';
  }

  // ── History ──
  addHistory(method, params, result, success, elapsed) {
    this.history.unshift({
      method,
      params,
      result,
      success,
      elapsed,
      timestamp: new Date()
    });
    document.getElementById('historyCount').textContent = this.history.length;
    this.renderHistory();
  }

  renderHistory() {
    const list = document.getElementById('historyList');
    if (!this.history.length) {
      list.innerHTML = '<p class="text-dim text-center">No requests yet</p>';
      return;
    }

    list.innerHTML = this.history.map((h, i) => `
      <div class="history-card ${h._expanded ? 'expanded' : ''}" onclick="app.toggleHistoryCard(${i})">
        <div class="history-header">
          <span class="history-method">${esc(h.method)}</span>
          <span class="history-time">${h.timestamp.toLocaleTimeString()} · ${h.elapsed}ms</span>
          <span class="history-status ${h.success ? 'success' : 'error'}">${h.success ? 'OK' : 'ERR'}</span>
        </div>
        <div class="history-detail">
          <div class="history-label">Request</div>
          <div class="json-view">${syntaxHighlight(JSON.stringify(h.params, null, 2))}</div>
          <div class="history-label" style="margin-top:10px">Response</div>
          <div class="json-view">${syntaxHighlight(JSON.stringify(h.result, null, 2))}</div>
        </div>
      </div>
    `).join('');
  }

  toggleHistoryCard(index) {
    this.history[index]._expanded = !this.history[index]._expanded;
    this.renderHistory();
  }

  clearHistory() {
    this.history = [];
    document.getElementById('historyCount').textContent = '0';
    this.renderHistory();
  }

  // ── Modal ──
  closeModal(event) {
    if (event && event.target !== document.getElementById('modalOverlay')) return;
    document.getElementById('modalOverlay').style.display = 'none';
  }

  // ── Status ──
  setConnectionStatus(state, text) {
    const status = document.getElementById('connectionStatus');
    const dot = status.querySelector('.status-dot');
    dot.className = `status-dot ${state}`;
    status.querySelector('span:last-child').textContent = text;
  }

  // ── Toast ──
  toast(message, type = 'info') {
    const container = document.getElementById('toastContainer');
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.textContent = message;
    container.appendChild(toast);
    setTimeout(() => {
      toast.style.opacity = '0';
      toast.style.transform = 'translateX(100%)';
      setTimeout(() => toast.remove(), 200);
    }, 3000);
  }
}


// ── Helpers ──
function esc(str) {
  const el = document.createElement('span');
  el.textContent = String(str);
  return el.innerHTML;
}

function syntaxHighlight(json) {
  if (!json) return '';
  return esc(json).replace(
    /("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g,
    (match) => {
      let cls = 'json-number';
      if (/^"/.test(match)) {
        if (/:$/.test(match)) {
          cls = 'json-key';
          // Remove the colon from the match for wrapping, add it back
          return `<span class="${cls}">${match.slice(0, -1)}</span>:`;
        } else {
          cls = 'json-string';
        }
      } else if (/true|false/.test(match)) {
        cls = 'json-boolean';
      } else if (/null/.test(match)) {
        cls = 'json-null';
      }
      return `<span class="${cls}">${match}</span>`;
    }
  );
}

// Keyboard shortcut — Escape closes modal
document.addEventListener('keydown', e => {
  if (e.key === 'Escape') app.closeModal();
});


// ── Init ──
const app = new App();
