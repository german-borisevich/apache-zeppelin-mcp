# Apache Zeppelin MCP Server

An MCP (Model Context Protocol) server that wraps the Apache Zeppelin REST API, allowing LLM agents in Claude Desktop and Claude Code to interact with Zeppelin notebooks.

## Prerequisites

- [uv](https://docs.astral.sh/uv/getting-started/installation/) installed
- A running Apache Zeppelin instance with REST API enabled
- Zeppelin credentials (username and password)

## Available Tools

| Tool | Description |
|------|-------------|
| `list_notebooks` | List all notebooks on the server |
| `search_notebooks` | Full-text search across all notebook paragraphs |
| `get_notebook` | Get full notebook details including paragraphs, code, and output |
| `create_notebook` | Create a new empty notebook |
| `add_paragraph` | Add a new paragraph to an existing notebook |
| `run_paragraph` | Run a paragraph synchronously and return the result |
| `run_all_paragraphs` | Run all paragraphs in a notebook asynchronously |
| `get_paragraph_status` | Check execution status of a paragraph |

For safety, delete and edit operations on existing paragraphs are deliberately not exposed.

## Setup for Claude Desktop

1. Open Claude Desktop settings and navigate to the MCP servers configuration file:
   - macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
   - Windows: `%APPDATA%\Claude\claude_desktop_config.json`

2. Add the Zeppelin server to the `mcpServers` section:

```json
{
  "mcpServers": {
    "zeppelin": {
      "command": "uv",
      "args": [
        "--directory", "/ABSOLUTE/PATH/TO/apache-zeppelin-mcp",
        "run", "server.py"
      ],
      "env": {
        "ZEPPELIN_BASE_URL": "http://your-zeppelin-host:8080",
        "ZEPPELIN_USERNAME": "your-username",
        "ZEPPELIN_PASSWORD": "your-password"
      }
    }
  }
}
```

Replace `/ABSOLUTE/PATH/TO/apache-zeppelin-mcp` with the actual absolute path to this project directory.

3. Restart Claude Desktop. The Zeppelin tools will appear in the tools menu (hammer icon).

## Setup for Claude Code

Run the following command to register the server:

```bash
claude mcp add zeppelin \
  -e ZEPPELIN_BASE_URL=http://your-zeppelin-host:8080 \
  -e ZEPPELIN_USERNAME=your-username \
  -e ZEPPELIN_PASSWORD=your-password \
  -- uv --directory /ABSOLUTE/PATH/TO/apache-zeppelin-mcp run server.py
```

Replace the URL, credentials, and path with your actual values.

To verify it was added:

```bash
claude mcp list
```

To remove it later:

```bash
claude mcp remove zeppelin
```

## Verifying the Connection

### 1. Check that the server starts

Run the server directly to confirm it starts without errors:

```bash
ZEPPELIN_BASE_URL=http://your-zeppelin-host:8080 \
ZEPPELIN_USERNAME=your-username \
ZEPPELIN_PASSWORD=your-password \
uv run server.py
```

If configuration is correct the process will start and wait for input on stdin (this is normal — it communicates via the MCP stdio protocol). Press `Ctrl+C` to stop.

If environment variables are missing you will see a `ValueError` immediately.

### 2. Test tools with MCP Inspector

The MCP Inspector provides a web UI for testing each tool interactively:

```bash
ZEPPELIN_BASE_URL=http://your-zeppelin-host:8080 \
ZEPPELIN_USERNAME=your-username \
ZEPPELIN_PASSWORD=your-password \
mcp dev server.py
```

This opens a browser where you can:
- See all 8 registered tools
- Call `list_notebooks` to verify the connection to Zeppelin is working
- Test `search_notebooks` with a keyword
- Try `get_notebook` with a notebook ID from the list
- Create a test notebook, add a paragraph, run it, and check the result

### 3. Test from Claude Desktop

After adding the server to `claude_desktop_config.json` and restarting Claude Desktop:

1. Open a new conversation
2. Click the hammer icon at the bottom of the input box — you should see all 8 Zeppelin tools listed
3. Ask Claude: *"List all my Zeppelin notebooks"*
4. Claude will call `list_notebooks` and show the results

If the tools don't appear, check the Claude Desktop logs:
- macOS: `~/Library/Logs/Claude/mcp*.log`
- Windows: `%APPDATA%\Claude\Logs\mcp*.log`

### 4. Test from Claude Code

After adding the server with `claude mcp add`:

1. Start Claude Code: `claude`
2. Ask: *"List all my Zeppelin notebooks"*
3. Claude will call `list_notebooks` — approve the tool call when prompted

### 5. End-to-end smoke test

Ask the agent to run through this sequence to fully verify all tools:

```
1. List all notebooks
2. Search for "select" (or any keyword likely in your notebooks)
3. Get the details of one notebook from the list
4. Create a new notebook called "MCP Test"
5. Add a paragraph with: %md Hello from MCP!
6. Run that paragraph
7. Check the paragraph status
```

If all steps succeed, the server is fully operational.

## Troubleshooting

| Problem | Cause | Fix |
|---------|-------|-----|
| `ValueError: ZEPPELIN_BASE_URL environment variable is required` | Missing env vars | Set all three env vars (`ZEPPELIN_BASE_URL`, `ZEPPELIN_USERNAME`, `ZEPPELIN_PASSWORD`) |
| `httpx.ConnectError` | Zeppelin is unreachable | Verify `ZEPPELIN_BASE_URL` is correct and Zeppelin is running |
| `HTTP 401/403` on every call | Wrong credentials | Check `ZEPPELIN_USERNAME` and `ZEPPELIN_PASSWORD` |
| Tools don't appear in Claude Desktop | Config error or server crash | Check the MCP log files and verify `claude_desktop_config.json` syntax |
| Tools don't appear in Claude Code | Server not registered | Run `claude mcp list` and re-add if missing |
