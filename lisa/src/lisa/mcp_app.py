"""Compose and run Lisa's MCP server."""

from .bootstrap import build_services
from .context import WorkspaceContext
from .controllers.mcp import create_server

mcp = create_server(build_services(WorkspaceContext.from_environment()))

if __name__ == "__main__":
    mcp.run()
