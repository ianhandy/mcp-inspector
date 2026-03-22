# MCP Inspector

A universal web-based inspector for Model Context Protocol (MCP) servers. Connect to any MCP server via Streamable HTTP or SSE transport, explore available tools/resources/prompts, and execute them interactively.

## Features

- **Connect to any MCP server** — Streamable HTTP and legacy SSE transport support
- **Interactive tool execution** — JSON Schema → auto-generated forms, or raw JSON mode
- **Resource browser** — Read and view server resources with content rendering
- **Prompt explorer** — View and test prompt templates with argument forms
- **Request history** — Full request/response log with timing data
- **Saved servers** — Persistent server list in localStorage
- **Beautiful UI** — FunForrest dark theme with particle effects and smooth animations

## Usage

1. Open `index.html` in a browser
2. Enter an MCP server URL (e.g., `http://localhost:3000/mcp`)
3. Select transport type (Streamable HTTP or SSE)
4. Click Connect
5. Explore tools, resources, and prompts in the tabbed interface

## Architecture

- `index.html` — Page structure
- `style.css` — FunForrest dark theme styling
- `app.js` — MCP client protocol implementation + UI logic

No build tools, no frameworks, no dependencies. Pure HTML/CSS/JS.

## MCP Protocol Support

- JSON-RPC 2.0 over Streamable HTTP (POST with JSON or SSE response)
- Legacy SSE transport with EventSource
- Session management via `mcp-session-id` header
- `initialize` / `tools/list` / `tools/call` / `resources/list` / `resources/read` / `prompts/list` / `prompts/get`
