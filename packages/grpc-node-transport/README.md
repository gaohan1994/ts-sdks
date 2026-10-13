# @mysten/grpc-node-transport

A native gRPC (HTTP/2) transport for `SuiGrpcClient` from `@mysten/sui/grpc`, built on
[`@grpc/grpc-js`](https://www.npmjs.com/package/@grpc/grpc-js). Node.js only.

Compared with the default gRPC-web transport, it:

- applies HTTP/2 flow control to server streams: a paused stream stops the server after a bounded
  number of messages,
- cancels the call on the server (`RST_STREAM`) when a stream is closed,
- connects to nodes that serve native gRPC without a gRPC-web proxy,
- sends keepalive pings while calls are open, so long-lived subscriptions detect dead connections.

## Installation

```bash
npm install @mysten/grpc-node-transport @mysten/sui
```

## Usage

```typescript
import { GrpcNodeTransport } from '@mysten/grpc-node-transport';
import { SuiGrpcClient } from '@mysten/sui/grpc';

const transport = new GrpcNodeTransport({ baseUrl: 'https://fullnode.mainnet.sui.io:443' });
const client = new SuiGrpcClient({ network: 'mainnet', transport });

for await (const result of client.streamCheckpoints()) {
	console.log(result.checkpoint.sequenceNumber);
}

// Release the HTTP/2 connection when done.
transport.close();
```

`https://` URLs connect with TLS and `http://` URLs without. Pass `channelCredentials` to use other
credentials and `clientOptions` for `@grpc/grpc-js` channel options.

See the
[gRPC client documentation](https://sdk.mystenlabs.com/sui/clients/grpc#native-grpc-transport) for
all options.
