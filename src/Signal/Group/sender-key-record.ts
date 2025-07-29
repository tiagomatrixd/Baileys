import { BufferJSON } from '../../Utils/generics'
import { SenderKeyState } from './sender-key-state'

export interface SenderKeyStateStructure {
	senderKeyId: number
	senderChainKey: {
		iteration: number
		seed: Uint8Array
	}
	senderSigningKey: {
		public: Uint8Array
		private?: Uint8Array
	}
	senderMessageKeys: Array<{
		iteration: number
		seed: Uint8Array
	}>
}

export class SenderKeyRecord {
	private readonly MAX_STATES = 5
	private readonly senderKeyStates: SenderKeyState[] = []

	constructor(serialized?: SenderKeyStateStructure[]) {
		if (serialized) {
			for (const structure of serialized) {
				this.senderKeyStates.push(new SenderKeyState(null, null, null, null, null, null, structure))
			}
		}
	}

	public isEmpty(): boolean {
		return this.senderKeyStates.length === 0
	}

	public getSenderKeyState(keyId?: number): SenderKeyState | undefined {
		if (keyId === undefined && this.senderKeyStates.length) {
			// Return the most recent valid state
			const recentState = this.senderKeyStates[this.senderKeyStates.length - 1]
			// Validate the state before returning
			if (recentState && this.isValidSenderKeyState(recentState)) {
				return recentState
			}
			// If recent state is invalid, try to find a valid one
			for (let i = this.senderKeyStates.length - 1; i >= 0; i--) {
				const state = this.senderKeyStates[i]
				if (state && this.isValidSenderKeyState(state)) {
					return state
				}
			}
			// If no valid state found, clear invalid states
			this.senderKeyStates.length = 0
			return undefined
		}

		const state = this.senderKeyStates.find(state => state && state.getKeyId() === keyId)
		// Validate the state before returning
		if (state && this.isValidSenderKeyState(state)) {
			return state
		}
		
		return undefined
	}

	private isValidSenderKeyState(state: SenderKeyState): boolean {
		try {
			// Basic validation checks
			if (!state) return false
			
			const keyId = state.getKeyId()
			if (typeof keyId !== 'number' || keyId <= 0) return false
			
			const chainKey = state.getSenderChainKey()
			if (!chainKey) return false
			
			const iteration = chainKey.getIteration()
			if (typeof iteration !== 'number' || iteration < 0) return false
			
			// Check if signing keys exist
			const publicKey = state.getSigningKeyPublic()
			if (!publicKey || publicKey.length === 0) return false
			
			return true
		} catch (error) {
			return false
		}
	}

	public addSenderKeyState(id: number, iteration: number, chainKey: Uint8Array, signatureKey: Uint8Array): void {
		this.senderKeyStates.push(new SenderKeyState(id, iteration, chainKey, null, signatureKey))
		if (this.senderKeyStates.length > this.MAX_STATES) {
			this.senderKeyStates.shift()
		}
	}

	public setSenderKeyState(
		id: number,
		iteration: number,
		chainKey: Uint8Array,
		keyPair: { public: Uint8Array; private: Uint8Array }
	): void {
		this.senderKeyStates.length = 0
		this.senderKeyStates.push(new SenderKeyState(id, iteration, chainKey, keyPair))
	}

	public serialize(): SenderKeyStateStructure[] {
		return this.senderKeyStates.map(state => state.getStructure())
	}
	static deserialize(data: Uint8Array | string | SenderKeyStateStructure[]): SenderKeyRecord {
		let parsed: SenderKeyStateStructure[]
		if (typeof data === 'string') {
			parsed = JSON.parse(data, BufferJSON.reviver)
		} else if (data instanceof Uint8Array) {
			const str = Buffer.from(data).toString('utf-8')
			parsed = JSON.parse(str, BufferJSON.reviver)
		} else {
			parsed = data
		}

		return new SenderKeyRecord(parsed)
	}
}
