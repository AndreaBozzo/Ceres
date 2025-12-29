# ceres-client

**The communication bridge of the Ceres ecosystem.**

`ceres-client` is the specialized module that allows Ceres to talk to external data sources. It acts as the "messenger" that retrieves valuable metadata from open data portals so they can be processed and indexed.

### Supported Protocols & Portals
* **CKAN Integration:** Seamlessly fetches data from Comprehensive Knowledge Archive Network portals.
* **HTTP Clients:** Robust handling of web-based data requests.
* **Gemini API**: This is Google's embeddings API, which is already integrated. It generates text embeddings for semantic search. 


### Why this matters
Without this client, Ceres wouldn't have any data to harvest. This crate ensures that connections to fragmented regional portals remain stable and efficient.

