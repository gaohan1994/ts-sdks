---
'@mysten/grpc-node-transport': minor
---

Add `@mysten/grpc-node-transport`, a native gRPC (HTTP/2) transport for `SuiGrpcClient` on Node.js built on `@grpc/grpc-js`. `GrpcNodeTransport` implements `PullServerStreamingTransport`, so ledger streams read messages on demand and HTTP/2 flow control stops the server while the consumer pauses (at most `prefetch`, default 16, decoded messages are buffered). Leaving a stream cancels the call on the server, errors carry the same status codes and decoded messages as the gRPC-web transport, and keepalive pings run while calls are open.
