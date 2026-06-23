#!/usr/bin/env python3
"""
Robinhood trading agent demo — powered by Pydantic AI.

Setup:
    pip install "pydantic-ai>=0.3" anthropic
    export ANTHROPIC_API_KEY=sk-ant-...
    export ROBINHOOD_TOKEN=...   # from Robinhood OAuth (see README)

Run:
    python -m robinhood_agent.demo
"""
from __future__ import annotations

import asyncio
import os
import sys


def _check_env() -> None:
    if not os.getenv("ANTHROPIC_API_KEY"):
        print("Error: ANTHROPIC_API_KEY is not set.")
        print("  export ANTHROPIC_API_KEY=sk-ant-...")
        sys.exit(1)

    if not os.getenv("ROBINHOOD_TOKEN"):
        print("Warning: ROBINHOOD_TOKEN is not set — Robinhood MCP calls will fail.")
        print("  Authenticate at https://agent.robinhood.com and set the token:")
        print("  export ROBINHOOD_TOKEN=...")
        print()


EXAMPLE_PROMPTS = [
    "What's in my portfolio?",
    "What is AAPL trading at right now?",
    "Should I buy NEE given current CT energy rates?",
    "Buy 1 share of AAPL",
]


async def run(agent, energy_context: str | None) -> None:
    print("─" * 50)
    print("  Robinhood Agent  (type 'quit' to exit)")
    print("─" * 50)

    if energy_context:
        print(f"\n{energy_context}\n")
        print("Energy rate data loaded — ask about energy stocks for context-aware analysis.\n")
    else:
        print()

    print("Example prompts:")
    for p in EXAMPLE_PROMPTS:
        print(f"  • {p}")
    print()

    # Energy context is prepended once to the first user message so the
    # agent has it in scope without repeating it on every turn.
    pending_context = energy_context

    async with agent.run_mcp_servers():
        while True:
            try:
                user_input = input("You: ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\nGoodbye.")
                break

            if not user_input or user_input.lower() in ("quit", "exit", "q"):
                print("Goodbye.")
                break

            prompt = user_input
            if pending_context:
                prompt = f"{pending_context}\n\nUser: {user_input}"
                pending_context = None

            try:
                result = await agent.run(prompt)
                print(f"\nAgent: {result.output}\n")
            except Exception as exc:
                print(f"\nError: {exc}\n")


async def main() -> None:
    _check_env()

    from robinhood_agent.agent import build_agent
    from robinhood_agent.energy_context import get_energy_summary

    agent = build_agent()
    energy_ctx = get_energy_summary()

    await run(agent, energy_ctx)


if __name__ == "__main__":
    asyncio.run(main())
