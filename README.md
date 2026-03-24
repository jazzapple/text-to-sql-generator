# Text-to-SQL

A CLI tool that converts natural language questions into Snowflake-compatible SQL queries using Claude.

## Features

- Natural language input → Snowflake SQL output
- Injects Snowflake role/database/schema context from environment variables
- SELECT-only guardrail enforced in both the system prompt and post-generation validation
- Local SQLite mock database for testing without a Snowflake connection

## Requirements

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)
- Anthropic API key

## Setup

```bash
# Install dependencies
uv sync

# Copy and configure environment variables
cp .env.example .env
# Add your ANTHROPIC_API_KEY to .env
```

### Environment variables

| Variable | Required | Description |
|---|---|---|
| `ANTHROPIC_API_KEY` | Yes | Your Anthropic API key |
| `CLAUDE_MODEL` | No | Model to use (default: `claude-haiku-4-5-20251001`) |
| `SNOWFLAKE_ROLE` | No | Snowflake role injected as context (e.g. `ANALYST`) |
| `SNOWFLAKE_DATABASE` | No | Target database |
| `SNOWFLAKE_SCHEMA` | No | Target schema |
| `SNOWFLAKE_WAREHOUSE` | No | Warehouse |
| `USE_MOCK_DB` | No | Set to `true` to run queries against the local SQLite mock |

## Usage

```bash
# Single question
uv run python src/text_to_sql.py "Show me the top 10 customers by revenue"

# Interactive mode
uv run python src/text_to_sql.py
```

### Example output

```
Generating SQL for: 'Show me the top 10 customers by revenue'

Generated SQL:
------------------------------------------------------------
SELECT
  c.customer_id,
  c.name,
  SUM(oi.quantity * oi.unit_price) AS total_revenue
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY c.customer_id, c.name
ORDER BY total_revenue DESC
LIMIT 10;
------------------------------------------------------------
```

## Guardrails

Only `SELECT` statements are generated. Any request for a destructive or data-modifying operation is blocked:

```bash
uv run python src/text_to_sql.py "delete all records from orders"
# Error: ERROR: Only SELECT queries are permitted.
```

## Mock database

Set `USE_MOCK_DB=true` to execute the generated SQL against a local in-memory SQLite database seeded with sample data. Useful for end-to-end testing without Snowflake access.

Tables: `customers`, `orders`, `order_items`, `products`

## Tests

```bash
uv run python -m pytest tests/
```

## Roadmap

- **LangChain integration** — rewrite prompt construction using LangChain's built-in prompt templates (`ChatPromptTemplate`, `PromptTemplate`) and output parsers (`StrOutputParser`, `PydanticOutputParser`) to replace the current hand-rolled string formatting
- **OpenAI model support** — add an `OPENAI_API_KEY` env var and swap in `ChatOpenAI` as an alternative LLM backend alongside the existing Anthropic/Claude backend, selectable via config
- **Structured output parsing** — use LangChain parsers to return a typed object (SQL string + explanation + confidence) instead of raw text, making downstream validation cleaner
