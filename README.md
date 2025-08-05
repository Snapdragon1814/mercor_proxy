# Mercor Proxy – Core Optimizations

## Caching
- Stores results to avoid repeated backend calls for the same sequence.

## Rate Limiting with Backoff
- Retries on 429 errors with backoff.

## Two Different Queues for Shorter and Larger Strings
- Shorter strings are batched separately from larger strings.
- The idea is to batch larger strings together before sending them in a batch.
