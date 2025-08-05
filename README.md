# Performance Results Summary

- **Total successful requests:** 27  
- **Total time taken:** 3.43 seconds

## Client A
- Successful requests: 15  
- Time taken: 2.47 seconds

## Client B
- Successful requests: 12  
- Time taken: 3.40 seconds

# Mercor Proxy – Core Optimizations

## Caching
- Stores results to avoid repeated backend calls for the same sequence.

## Rate Limiting with Backoff
- Retries on 429 errors with backoff.

## Two Different Queues for Shorter and Larger Strings
- Shorter strings are batched separately from larger strings.
- The idea is to batch larger strings together before sending them in a batch.
