---
title: Embeddings and costs
description: Embeddings in Ceres are optional, and local Ollama keeps the default path cost-free
---

# Embeddings are optional

Ceres does not require embeddings to be useful.

You can run the full harvesting pipeline in metadata-only mode, keep a synchronized catalog in PostgreSQL, export it, and operate the API without ever generating vectors.

## Recommended path: local Ollama

If you want semantic search, the preferred setup is local Ollama:

```bash
ollama serve
ollama pull nomic-embed-text

export EMBEDDING_PROVIDER=ollama
export OLLAMA_ENDPOINT=http://localhost:11434
ceres embed
```

That gives you:

- zero per-request embedding cost
- a clean separation between harvesting and embedding runs
- the option to backfill vectors only when you decide the catalog is ready

## When cloud providers still make sense

Gemini and OpenAI are still supported when you want managed infrastructure or different model characteristics. In that case, your cost is driven by the standalone embedding pass, not by harvesting itself.

Typical pattern:

1. Harvest with `--metadata-only`
2. Decide whether a portal or snapshot is worth embedding
3. Run `ceres embed` with your chosen provider

That keeps cloud spend explicit and isolated.

## Cost shape by workflow

| Metric | Detail |
|--------------------------------|--------------------------------------------------------------------------------|
| Harvest only | No embedding cost |
| Harvest + local Ollama | No per-request cost, only your own CPU/GPU/runtime cost |
| Harvest + Gemini/OpenAI | Pay only for the texts you choose to embed |
| Re-sync after first run | Usually cheap because incremental sync and delta detection reduce re-embedding |

## Why later runs are cheaper

Ceres uses two mechanisms to keep optional embedding passes under control:

- incremental sync reduces the amount of portal data fetched
- content-hash delta detection skips re-embedding when the embeddable text did not actually change

That matters whether you run Ollama locally or use a hosted provider.

## Practical guidance

- If your goal is catalog maintenance, stay in metadata-only mode.
- If your goal is semantic retrieval, start with Ollama.
- If you need hosted inference, harvest first and embed second so the spend is isolated and reversible.

The important shift is operational: in Ceres, embeddings are now an optional downstream stage, not a prerequisite for using the project.
