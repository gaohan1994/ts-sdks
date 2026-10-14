// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, Client, Metadata } from '@grpc/grpc-js';
import type {
	CallOptions,
	ClientOptions,
	ClientReadableStream,
	ServiceError,
	StatusObject,
} from '@grpc/grpc-js';
import {
	GrpcStatusCode,
	markGrpcStatusMessageDecoded,
	markNonRetryable,
	normalizeGrpcError,
	RpcError,
} from '@mysten/sui/grpc';
import type { PullServerStreamingTransport, ServerStreamingIterableCall } from '@mysten/sui/grpc';
import {
	Deferred,
	mergeRpcOptions,
	RpcOutputStreamController,
	ServerStreamingCall,
	UnaryCall,
} from '@protobuf-ts/runtime-rpc';
import type {
	ClientStreamingCall,
	DuplexStreamingCall,
	MethodInfo,
	RpcMetadata,
	RpcOptions,
	RpcStatus,
} from '@protobuf-ts/runtime-rpc';

/** Default for `maxResponseMessageBytes`, the same as `GrpcWebFetchTransport`'s. */
export const DEFAULT_MAX_RESPONSE_MESSAGE_BYTES = 64 * 1024 * 1024;

/**
 * Channel options applied before `clientOptions`. Keepalive pings run only while a call is open, so
 * a long-lived subscription notices a dead connection without pinging idle channels.
 */
export const DEFAULT_CLIENT_OPTIONS: Readonly<ClientOptions> = {
	'grpc.keepalive_time_ms': 60_000,
	'grpc.keepalive_timeout_ms': 20_000,
	'grpc.keepalive_permit_without_calls': 0,
};

export interface GrpcNodeTransportOptions extends Pick<
	RpcOptions,
	'meta' | 'timeout' | 'interceptors' | 'binaryOptions'
> {
	/**
	 * The node's gRPC endpoint, such as `https://fullnode.mainnet.sui.io:443`. `https:` connects with
	 * TLS and `http:` without; the port defaults to 443 and 80. A path is not allowed.
	 */
	baseUrl: string;
	/**
	 * Replaces the credentials chosen from the `baseUrl` scheme, for example to add client
	 * certificates.
	 */
	channelCredentials?: ChannelCredentials;
	/** `@grpc/grpc-js` channel options, applied over `DEFAULT_CLIENT_OPTIONS`. */
	clientOptions?: ClientOptions;
	/**
	 * Largest response message accepted, in bytes. A larger message fails the call with
	 * `RESOURCE_EXHAUSTED`. Sets `grpc.max_receive_message_length` unless `clientOptions` sets it.
	 * Defaults to 64 MiB.
	 */
	maxResponseMessageBytes?: number;
	/**
	 * Decoded messages a server stream may hold ahead of the consumer. Defaults to 16, the object-mode
	 * high-water mark of Node.js streams, which `@grpc/grpc-js` does not expose as an option. Another
	 * value is set on the stream's internal state when that state has the expected shape, and is
	 * ignored otherwise. Bytes not yet decoded are bounded separately by the HTTP/2 stream window
	 * (`grpc-node.flow_control_window` in `clientOptions`, 64 KiB by default).
	 */
	prefetch?: number;
}

type GrpcOptions = Omit<
	GrpcNodeTransportOptions,
	'baseUrl' | 'channelCredentials' | 'clientOptions'
>;

const OK = GrpcStatusCode.OK as number;

function statusName(code: number): string {
	return GrpcStatusCode[code] ?? GrpcStatusCode[GrpcStatusCode.UNKNOWN];
}

function parseBaseUrl(baseUrl: string): { target: string; secure: boolean } {
	const url = new URL(baseUrl);
	const secure = url.protocol === 'https:';

	if (!secure && url.protocol !== 'http:') {
		throw new TypeError(`Expected an http: or https: gRPC URL, got ${baseUrl}`);
	}
	if (url.pathname !== '/' || url.search || url.hash) {
		throw new TypeError(`A native gRPC URL cannot have a path, query or fragment: ${baseUrl}`);
	}

	return { target: `${url.hostname}:${url.port || (secure ? '443' : '80')}`, secure };
}

function methodPath(method: MethodInfo): string {
	return `/${method.service.typeName}/${method.name}`;
}

// protobuf-ts carries `-bin` values as base64 text, like gRPC-web headers.
function toGrpcMetadata(meta: RpcMetadata | undefined): Metadata {
	const metadata = new Metadata();

	for (const [key, value] of Object.entries(meta ?? {})) {
		for (const item of Array.isArray(value) ? value : [value]) {
			metadata.add(key, key.endsWith('-bin') ? Buffer.from(item, 'base64') : item);
		}
	}

	return metadata;
}

function fromGrpcMetadata(metadata: Metadata | undefined): RpcMetadata {
	const out: RpcMetadata = {};

	for (const [key, values] of Object.entries(metadata?.toJSON() ?? {})) {
		const strings = values.map((value) =>
			typeof value === 'string' ? value : Buffer.from(value).toString('base64'),
		);
		out[key] = strings.length === 1 ? strings[0] : strings;
	}

	return out;
}

function callOptions(options: RpcOptions): CallOptions {
	const { timeout } = options;

	if (timeout === undefined) return {};

	return { deadline: typeof timeout === 'number' ? Date.now() + timeout : timeout };
}

// `@grpc/grpc-js` rejects a response message over `grpc.max_receive_message_length` with one of
// these texts: `Received message larger than max (<size> vs <limit>)` for its wire size
// (`build/src/stream-decoder.js`), or `Received message that decompresses to a size larger than
// <limit>` for a compressed message (`build/src/compression-filter.js`).
const RECEIVE_LIMIT =
	/^Received message (?:larger than max \(|that decompresses to a size larger than \d)/;
// grpc-js reports a reset or closed connection as INTERNAL with one of these messages.
const CONNECTION_LOST =
	/RST_STREAM|GOAWAY|without gRPC status|Session closed|socket hang up|ECONNRESET/;

function reasonMessage(reason: unknown): string {
	if (typeof reason === 'string') return reason;

	try {
		const message = (reason as { message?: unknown } | null)?.message;
		if (typeof message === 'string') return message;
	} catch {
		// An abort reason is arbitrary caller data.
	}

	return 'The operation was aborted';
}

function withMethod(error: RpcError, method: MethodInfo): RpcError {
	error.methodName = method.name;
	error.serviceName = method.service.typeName;

	return error;
}

// Coded the way the gRPC-web transport codes an aborted call: `CANCELLED` with the reason's text,
// re-coded from the reason by `normalizeGrpcError`.
function abortError(signal: AbortSignal, method: MethodInfo): RpcError {
	const error = withMethod(
		new RpcError(reasonMessage(signal.reason), statusName(GrpcStatusCode.CANCELLED)),
		method,
	);
	normalizeGrpcError(error, signal);

	return error;
}

function toRpcError(error: unknown, method: MethodInfo, signal: AbortSignal | undefined): RpcError {
	let rpcError: RpcError;

	if (error instanceof RpcError) {
		rpcError = error;
	} else if (typeof (error as Partial<ServiceError> | null)?.code === 'number') {
		// `@grpc/grpc-js` has percent-decoded `details` with `decodeURI`. gRPC servers escape only `%`
		// and bytes outside printable ASCII, which `decodeURI` decodes as `decodeURIComponent` does.
		const { code, details, metadata } = error as ServiceError;
		// A reset or closed connection arrives as INTERNAL; the gRPC-web transport reports it as
		// UNAVAILABLE so streams reconnect, and the same applies here.
		const lost =
			code === (GrpcStatusCode.INTERNAL as number) && CONNECTION_LOST.test(details ?? '');
		rpcError = new RpcError(
			details || statusName(code),
			statusName(lost ? GrpcStatusCode.UNAVAILABLE : code),
			fromGrpcMetadata(metadata),
		);
		markGrpcStatusMessageDecoded(rpcError);
		// The same message would arrive again on retry, so it is not retryable.
		if (
			code === (GrpcStatusCode.RESOURCE_EXHAUSTED as number) &&
			RECEIVE_LIMIT.test(details ?? '')
		) {
			markNonRetryable(rpcError);
		}
	} else {
		rpcError = new RpcError(
			error instanceof Error ? error.message : String(error),
			statusName(GrpcStatusCode.INTERNAL),
		);
		markGrpcStatusMessageDecoded(rpcError);
	}

	withMethod(rpcError, method);
	normalizeGrpcError(rpcError, signal);

	return rpcError;
}

function okStatus(status: StatusObject): RpcStatus {
	return { code: statusName(OK), detail: status.details || statusName(OK) };
}

function setReadAhead(stream: object, prefetch: number): void {
	const state = (stream as { _readableState?: { highWaterMark?: unknown } })._readableState;

	if (state && typeof state.highWaterMark === 'number') state.highWaterMark = prefetch;
}

/**
 * A native gRPC (HTTP/2) transport for `SuiGrpcClient` on `@grpc/grpc-js`, for Node.js.
 *
 * Server streams are read with the stream paused, so `@grpc/grpc-js` stops reading the HTTP/2
 * stream once `prefetch` messages are waiting, and HTTP/2 flow control then stops the server.
 * Leaving a stream sends `RST_STREAM`, which cancels the call on the server.
 *
 * Errors are `RpcError`s coded and worded like `GrpcWebFetchTransport`'s. Client-streaming and
 * duplex calls are not supported. Call `close()` to release the channel.
 */
export class GrpcNodeTransport implements PullServerStreamingTransport {
	readonly #client: Client;
	readonly #defaults: RpcOptions;
	readonly #prefetch: number | undefined;
	#closed = false;

	constructor({
		baseUrl,
		channelCredentials,
		clientOptions,
		maxResponseMessageBytes = DEFAULT_MAX_RESPONSE_MESSAGE_BYTES,
		prefetch,
		...defaults
	}: GrpcNodeTransportOptions) {
		if (prefetch !== undefined && !(Number.isInteger(prefetch) && prefetch >= 1)) {
			throw new RangeError(`prefetch must be a positive integer, got ${prefetch}`);
		}

		const { target, secure } = parseBaseUrl(baseUrl);
		this.#client = new Client(
			target,
			channelCredentials ??
				(secure ? ChannelCredentials.createSsl() : ChannelCredentials.createInsecure()),
			{
				...DEFAULT_CLIENT_OPTIONS,
				'grpc.max_receive_message_length': maxResponseMessageBytes,
				...clientOptions,
			},
		);
		this.#prefetch = prefetch;
		this.#defaults = defaults satisfies GrpcOptions as RpcOptions;
	}

	mergeOptions(options?: Partial<RpcOptions>): RpcOptions {
		return mergeRpcOptions(this.#defaults, options);
	}

	/**
	 * Closes the channel. Calls in progress continue; new calls fail with a non-retryable
	 * `UNAVAILABLE`.
	 */
	close(): void {
		this.#closed = true;
		this.#client.close();
	}

	// `@grpc/grpc-js` throws when a call starts on a closed channel.
	#startError(error: unknown, method: MethodInfo, signal: AbortSignal | undefined): RpcError {
		if (!this.#closed || error instanceof RpcError) return toRpcError(error, method, signal);

		// A closed transport cannot recover, so retrying the call would fail the same way.
		return markNonRetryable(
			toRpcError(
				{ code: GrpcStatusCode.UNAVAILABLE, details: 'The transport was closed' },
				method,
				signal,
			),
		);
	}

	unary<I extends object, O extends object>(
		method: MethodInfo<I, O>,
		input: I,
		options: RpcOptions,
	): UnaryCall<I, O> {
		const signal = options.abort;
		const headers = new Deferred<RpcMetadata>();
		const response = new Deferred<O>();
		const status = new Deferred<RpcStatus>();
		const trailers = new Deferred<RpcMetadata>();
		const fail = (error: RpcError) => {
			for (const deferred of [headers, response, status, trailers]) deferred.rejectPending(error);
		};

		if (signal?.aborted) {
			fail(abortError(signal, method));
		} else {
			try {
				const onAbort = () => {
					fail(abortError(signal!, method));
					call.cancel();
				};
				const call = this.#client.makeUnaryRequest<I, O>(
					methodPath(method),
					(value) => Buffer.from(method.I.toBinary(value, options.binaryOptions)),
					(bytes) => method.O.fromBinary(bytes, options.binaryOptions),
					input,
					toGrpcMetadata(options.meta),
					callOptions(options),
					(error, value) => {
						if (error) fail(toRpcError(error, method, signal));
						else if (value !== undefined) response.resolvePending(value);
					},
				);
				signal?.addEventListener('abort', onAbort, { once: true });
				call.on('metadata', (metadata: Metadata) =>
					headers.resolvePending(fromGrpcMetadata(metadata)),
				);
				call.on('status', (result: StatusObject) => {
					signal?.removeEventListener('abort', onAbort);
					if (result.code !== OK) return;
					headers.resolvePending({});
					status.resolvePending(okStatus(result));
					trailers.resolvePending(fromGrpcMetadata(result.metadata));
				});
			} catch (error) {
				fail(this.#startError(error, method, signal));
			}
		}

		return new UnaryCall(
			method,
			options.meta ?? {},
			input,
			headers.promise,
			response.promise,
			status.promise,
			trailers.promise,
		);
	}

	/**
	 * A server-streaming call whose messages are read from the network only while the consumer
	 * waits on `responses.next()`, plus up to `prefetch` messages ahead. Interceptors are not applied.
	 */
	serverStreamingIterable<I extends object, O extends object>(
		method: MethodInfo<I, O>,
		input: I,
		options: RpcOptions,
	): ServerStreamingIterableCall<I, O> {
		const signal = options.abort;
		const headers = new Deferred<RpcMetadata>();
		const status = new Deferred<RpcStatus>();
		const trailers = new Deferred<RpcMetadata>();

		type State = 'open' | 'done' | 'failed' | 'cancelled';
		let state: State = 'open';
		// A call, so TypeScript does not narrow `state` across awaits.
		const phase = (): State => state;
		let failure: RpcError | undefined;
		// A server status that ends the call once the messages before it are consumed.
		let serverError: RpcError | undefined;
		let finalStatus: StatusObject | undefined;
		let stream: ClientReadableStream<O> | undefined;
		let wake: (() => void) | undefined;

		const notify = () => {
			const resume = wake;
			wake = undefined;
			resume?.();
		};

		const onAbort = () => fail(abortError(signal!, method));

		const settle = (next: 'failed' | 'cancelled', error: RpcError) => {
			state = next;
			headers.rejectPending(error);
			status.rejectPending(error);
			trailers.rejectPending(error);
			signal?.removeEventListener('abort', onAbort);
			stream?.cancel();
			notify();
		};

		const fail = (error: RpcError) => {
			if (state !== 'open') return;
			failure = error;
			settle('failed', error);
		};

		if (signal?.aborted) {
			fail(abortError(signal, method));
		} else {
			try {
				stream = this.#client.makeServerStreamRequest<I, O>(
					methodPath(method),
					(value) => Buffer.from(method.I.toBinary(value, options.binaryOptions)),
					(bytes) => method.O.fromBinary(bytes, options.binaryOptions),
					input,
					toGrpcMetadata(options.meta),
					callOptions(options),
				);
				if (this.#prefetch !== undefined) setReadAhead(stream, this.#prefetch);
				// A `readable` listener keeps the stream paused from the start: `@grpc/grpc-js` asks the
				// HTTP/2 stream for another message only while fewer than `prefetch` are buffered, so
				// waiting before the first pull reads no further ahead than waiting between pulls.
				stream.on('readable', notify);
				signal?.addEventListener('abort', onAbort, { once: true });
				stream.on('metadata', (metadata: Metadata) =>
					headers.resolvePending(fromGrpcMetadata(metadata)),
				);
				// Emitted before `status` for any status other than OK.
				stream.on('error', (error) => {
					if (state !== 'open' || serverError) return;
					serverError = toRpcError(error, method, signal);
					headers.rejectPending(serverError);
					notify();
				});
				// The abort listener stays attached until the iterator settles, so an abort still ends a
				// call whose status has arrived while messages remain buffered.
				stream.on('status', (result: StatusObject) => {
					if (result.code !== OK) return;
					finalStatus = result;
					headers.resolvePending({});
					notify();
				});
			} catch (error) {
				fail(this.#startError(error, method, signal));
			}
		}

		const done = { done: true, value: undefined } as const;

		const pull = async (): Promise<IteratorResult<O, undefined>> => {
			for (;;) {
				if (phase() === 'failed') throw failure;
				if (phase() !== 'open') return done;

				const message: O | null = stream!.read();
				if (message !== null) return { done: false, value: message };

				// The status follows the last message, so an empty buffer here is the end of the stream.
				if (serverError) {
					fail(serverError);
					throw failure;
				}
				if (finalStatus) {
					state = 'done';
					signal?.removeEventListener('abort', onAbort);
					status.resolvePending(okStatus(finalStatus));
					trailers.resolvePending(fromGrpcMetadata(finalStatus.metadata));
					return done;
				}

				await new Promise<void>((resolve) => (wake = resolve));
			}
		};

		// `next()` calls are serialized; `return()` is not, so it can cancel a pending read.
		let tail: Promise<unknown> = Promise.resolve();
		const responses: AsyncIterableIterator<O> = {
			[Symbol.asyncIterator]() {
				return this;
			},
			next() {
				const result = tail.then(pull);
				tail = result.catch(() => {});
				return result;
			},
			async return() {
				if (state === 'open') {
					settle(
						'cancelled',
						withMethod(
							new RpcError('stream cancelled by consumer', statusName(GrpcStatusCode.CANCELLED)),
							method,
						),
					);
				}
				return done;
			},
		};

		return {
			method,
			requestHeaders: options.meta ?? {},
			request: input,
			responses,
			headers: headers.promise,
			status: status.promise,
			trailers: trailers.promise,
		};
	}

	/**
	 * The push-based protobuf-ts contract used by the generated service clients. Messages are read
	 * as fast as they arrive; `serverStreamingIterable` reads on demand.
	 */
	serverStreaming<I extends object, O extends object>(
		method: MethodInfo<I, O>,
		input: I,
		options: RpcOptions,
	): ServerStreamingCall<I, O> {
		const call = this.serverStreamingIterable(method, input, options);
		const controller = new RpcOutputStreamController<O>();

		for (const promise of [call.headers, call.status, call.trailers]) void promise.catch(() => {});
		void (async () => {
			try {
				for await (const message of call.responses) controller.notifyMessage(message);
				controller.notifyComplete();
			} catch (error) {
				controller.notifyError(error as Error);
			}
		})();

		return new ServerStreamingCall(
			method,
			call.requestHeaders,
			input,
			call.headers,
			controller,
			call.status,
			call.trailers,
		);
	}

	clientStreaming<I extends object, O extends object>(
		method: MethodInfo<I, O>,
	): ClientStreamingCall<I, O> {
		throw withMethod(
			new RpcError(
				'Client streaming is not supported by GrpcNodeTransport',
				statusName(GrpcStatusCode.UNIMPLEMENTED),
			),
			method,
		);
	}

	duplex<I extends object, O extends object>(method: MethodInfo<I, O>): DuplexStreamingCall<I, O> {
		throw withMethod(
			new RpcError(
				'Duplex streaming is not supported by GrpcNodeTransport',
				statusName(GrpcStatusCode.UNIMPLEMENTED),
			),
			method,
		);
	}
}
