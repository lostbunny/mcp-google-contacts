"""MCP Google Contacts Server.

A server that provides Google Contacts functionality through the Machine
Conversation Protocol (MCP).
"""

import argparse
import os
from pathlib import Path

from mcp.server.fastmcp import FastMCP

from config import config
from tools import init_service, register_tools
import tools as _tools


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="MCP Google Contacts Server")
    parser.add_argument(
        "--transport",
        choices=["stdio", "http", "streamable-http", "sse"],
        default="stdio",
        help="Transport protocol to use (default: stdio). 'http' is kept as an alias for 'streamable-http'.",
    )
    parser.add_argument(
        "--host", default="localhost", help="Host for HTTP transport (default: localhost)"
    )
    parser.add_argument(
        "--port", type=int, default=8000, help="Port for HTTP transport (default: 8000)"
    )
    parser.add_argument(
        "--client-id", help="Google OAuth client ID (overrides environment variable)"
    )
    parser.add_argument(
        "--client-secret", help="Google OAuth client secret (overrides environment variable)"
    )
    parser.add_argument(
        "--refresh-token", help="Google OAuth refresh token (overrides environment variable)"
    )
    parser.add_argument("--credentials-file", help="Path to Google OAuth credentials.json file")

    # Credential manager selection
    parser.add_argument(
        "--cred-manager",
        choices=["1password"],
        default=None,
        help="Credential manager backend to use for client credentials and refresh token",
    )

    # 1Password-specific options (used when --cred-manager=1password)
    parser.add_argument(
        "--op-path",
        default="op",
        help="Path to the 1Password CLI executable (default: op)",
    )
    parser.add_argument(
        "--op-vault",
        default=None,
        help="1Password vault name (e.g. DevVault)",
    )
    parser.add_argument(
        "--op-oauth-item",
        default=None,
        help="1Password item name holding client_id (username) and client_secret (password)",
    )
    parser.add_argument(
        "--op-refresh-token-item",
        default=None,
        help="1Password item name holding the refresh token (password field)",
    )

    return parser.parse_args()


def main():
    """Run the MCP server."""
    print("Starting Google Contacts MCP Server...")

    args = parse_args()

    # Update config based on arguments
    if args.client_id:
        os.environ["GOOGLE_CLIENT_ID"] = args.client_id
    if args.client_secret:
        os.environ["GOOGLE_CLIENT_SECRET"] = args.client_secret
    if args.refresh_token:
        os.environ["GOOGLE_REFRESH_TOKEN"] = args.refresh_token

    # Handle credentials file argument
    if args.credentials_file:
        credentials_path = Path(args.credentials_file)
        if credentials_path.exists():
            # Add the specified credentials file to the beginning of the search paths
            config.credentials_paths.insert(0, credentials_path)
            print(f"Using credentials file: {credentials_path}")
        else:
            print(f"Warning: Specified credentials file {credentials_path} not found")

    # Wire up credential manager if requested
    if args.cred_manager == "1password":
        from credential_manager import OnePasswordCredentialManager

        missing = [
            name for name, val in [
                ("--op-vault", args.op_vault),
                ("--op-oauth-item", args.op_oauth_item),
                ("--op-refresh-token-item", args.op_refresh_token_item),
            ]
            if not val
        ]
        if missing:
            print(f"Error: --cred-manager=1password requires: {', '.join(missing)}")
            raise SystemExit(1)

        _tools._cred_manager = OnePasswordCredentialManager(
            op_path=args.op_path,
            vault=args.op_vault,
            oauth_item=args.op_oauth_item,
            refresh_token_item=args.op_refresh_token_item,
        )
        print(
            f"Using 1Password credential manager "
            f"(vault={args.op_vault}, oauth_item={args.op_oauth_item}, "
            f"refresh_token_item={args.op_refresh_token_item})"
        )

    transport = "streamable-http" if args.transport == "http" else args.transport

    # Initialize FastMCP server
    if transport == "stdio":
        mcp = FastMCP("google-contacts")
    else:
        mcp = FastMCP("google-contacts", host=args.host, port=args.port)

    # Register all tools
    register_tools(mcp)

    # Initialize service with credentials
    service = init_service()

    if not service:
        print("Warning: No valid Google credentials found. Authentication will be required.")
        print("You can provide credentials using environment variables or command line arguments:")
        print("  GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN")
        print("  --client-id, --client-secret, --refresh-token, --credentials-file")

    # Run the MCP server with the specified transport
    if transport == "stdio":
        print("Running with stdio transport")
        mcp.run(transport="stdio")
    else:
        print(f"Running with {transport} transport on {args.host}:{args.port}")
        mcp.run(transport=transport)


if __name__ == "__main__":
    main()
