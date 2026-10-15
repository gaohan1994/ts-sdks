// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

import { SuiGrpcClient } from '@mysten/sui/grpc';
import { beforeEach, describe, expect, it, vi } from 'vitest';

let resolveNameServiceName: typeof import('../../../src/utils/name.js').resolveNameServiceName;
let client: SuiGrpcClient;
const address = '0x123';

beforeEach(async () => {
	vi.resetModules();
	({ resolveNameServiceName } = await import('../../../src/utils/name.js'));
	client = new SuiGrpcClient({ network: 'mainnet', baseUrl: 'http://localhost' });
});

describe('resolveNameServiceName', () => {
	it.each([
		['mysten.sui', '@mysten'],
		['user.namespace.sui', 'user@namespace'],
		['User@Namespace', 'user@namespace'],
	])('returns the normalized name on the first and cached lookup (%s)', async (name, expected) => {
		const lookup = vi.spyOn(client.core, 'defaultNameServiceName').mockResolvedValue({
			data: { name },
		});

		const first = await resolveNameServiceName(client, address);
		const cached = await resolveNameServiceName(client, address);

		expect(first).toBe(expected);
		expect(cached).toBe(expected);
		expect(lookup).toHaveBeenCalledExactlyOnceWith({ address });
	});

	it('returns and caches null when the address has no name', async () => {
		const lookup = vi.spyOn(client.core, 'defaultNameServiceName').mockResolvedValue({
			data: { name: null },
		});

		await expect(resolveNameServiceName(client, address)).resolves.toBeNull();
		await expect(resolveNameServiceName(client, address)).resolves.toBeNull();
		expect(lookup).toHaveBeenCalledOnce();
	});

	it('returns and caches null when the lookup fails', async () => {
		const lookup = vi
			.spyOn(client.core, 'defaultNameServiceName')
			.mockRejectedValue(new Error('Name service unavailable'));

		await expect(resolveNameServiceName(client, address)).resolves.toBeNull();
		await expect(resolveNameServiceName(client, address)).resolves.toBeNull();
		expect(lookup).toHaveBeenCalledOnce();
	});
});
