from __future__ import annotations

import os

from pydantic_ai import Agent
from pydantic_ai.mcp import MCPServerHTTP

ROBINHOOD_MCP_URL = "https://agent.robinhood.com/mcp/trading"

SYSTEM_PROMPT = """\
You are a concise Robinhood trading assistant.
- Before executing any trade, state the current price and confirm with the user first.
- When CT energy market data is provided, use it to inform analysis of energy sector stocks.
  Relevant tickers: NEE (NextEra Energy), CEG (Constellation Energy), EXC (Exelon),
  AES (AES Corp), PCG (PG&E), SO (Southern Company).
- Keep responses brief and actionable. Flag risks clearly.
"""


def build_agent(token: str | None = None) -> Agent:
    t = token or os.getenv("ROBINHOOD_TOKEN")
    headers = {"Authorization": f"Bearer {t}"} if t else {}

    server = MCPServerHTTP(url=ROBINHOOD_MCP_URL, headers=headers)

    return Agent(
        "anthropic:claude-sonnet-4-6",
        mcp_servers=[server],
        system_prompt=SYSTEM_PROMPT,
    )
