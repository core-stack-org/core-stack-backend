"""Run the sample MCP server over stdio and print three tool calls.

Usage:
  CORE_STACK_API_KEY=your-key CORE_STACK_BASE_URL=http://127.0.0.1:8000 \\
    .venv/bin/python try_it.py
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client


def _text(result) -> str:
    if getattr(result, "structured_content", None) is not None:
        return json.dumps(result.structured_content, indent=2)
    parts = []
    for block in result.content or []:
        text = getattr(block, "text", None)
        if text:
            parts.append(text)
    return "\n".join(parts)


async def _call(session: ClientSession, name: str, arguments: dict) -> None:
    result = await session.call_tool(name, arguments)
    print(f"\n## {name} {arguments}")
    print(f"is_error={result.is_error}")
    print(_text(result)[:6000])


async def main() -> None:
    if not os.environ.get("CORE_STACK_API_KEY"):
        sys.exit("Set CORE_STACK_API_KEY before running try_it.py")
    here = Path(__file__).resolve().parent
    params = StdioServerParameters(
        command=sys.executable,
        args=[str(here / "server.py")],
        env=dict(os.environ),
        cwd=here,
    )
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = await session.list_tools()
            print("tools:", ", ".join(tool.name for tool in tools.tools))
            await _call(session, "list_public_apis", {"group": "dataset"})
            await _call(
                session,
                "describe_public_api",
                {"api_id": "get_tehsil_data", "property_name": "drought"},
            )
            await _call(
                session,
                "describe_public_api",
                {"api_id": "get_mws_data"},
            )
            await _call(
                session,
                "call_public_api",
                {
                    "api_id": "get_tehsil_data",
                    "query": {
                        "state": "Rajasthan",
                        "district": "Sirohi",
                        "tehsil": "Abu Road",
                        "data": "mws",
                    },
                },
            )
            await _call(
                session,
                "call_public_api",
                {
                    "api_id": "get_tehsil_data",
                    "query": {
                        "state": "Rajasthan",
                        "district": "Sirohi",
                        "tehsil": "Abu Road",
                        "data": "not_a_sheet",
                    },
                },
            )


if __name__ == "__main__":
    asyncio.run(main())
