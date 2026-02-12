# Cost-Effectiveness

API costs, based on the Gemini embedding model, are almost negligible, making the solution extremely efficient even for personal projects or those with limited budgets.

The main cost is for the initial creation of vector embeddings. Below is a cost breakdown for a large catalog.

## Cost Analysis for Initial Indexing

This scenario estimates the one-time cost to index a catalog of 50,000 datasets.

| Metric | Detail |
|--------------------------------|--------------------------------------------------------------------------------|
| **Cost per 1M Input Tokens** | ~$0.15 USD (Standard rate for Google's `gemini-embedding-001` model) |
| **Estimated Tokens per Dataset** | 500 tokens (A generous estimate for title, description, and tags) |
| **Total Tokens** | `50,000 datasets * 500 tokens/dataset = 25,000,000 tokens` |
| **Total Initial Cost** | `(25,000,000 / 1,000,000) * $0.15 =` **$3.75** |

As shown, the initial cost to index a substantial number of datasets is just a few dollars. Monthly maintenance for incremental updates would be even lower, typically amounting to a few cents.
