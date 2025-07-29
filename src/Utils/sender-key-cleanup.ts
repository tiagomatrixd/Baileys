import type { SignalAuthState } from '../Types'

/**
 * Utility functions for cleaning up corrupted sender keys and memory
 */

export interface SenderKeyCleanupOptions {
	/** Maximum age of sender key memory entries in milliseconds (default: 24 hours) */
	maxMemoryAge?: number
	/** Maximum number of sender key states to keep per group (default: 5) */
	maxStatesPerGroup?: number
	/** Whether to perform aggressive cleanup (default: false) */
	aggressive?: boolean
}

/**
 * Clean up corrupted or old sender keys from storage
 */
export async function cleanupSenderKeys(
	authState: SignalAuthState,
	options: SenderKeyCleanupOptions = {}
): Promise<void> {
	const {
		maxMemoryAge = 24 * 60 * 60 * 1000, // 24 hours
		maxStatesPerGroup = 5,
		aggressive = false
	} = options

	try {
		// Clean up sender key memory
		await cleanupSenderKeyMemory(authState, maxMemoryAge)
		
		// Clean up corrupted sender keys
		if (aggressive) {
			await cleanupCorruptedSenderKeys(authState, maxStatesPerGroup)
		}
	} catch (error) {
		// Don't fail if cleanup fails, just log the error
		console.warn('Failed to cleanup sender keys:', error)
	}
}

/**
 * Clean up old sender key memory entries
 */
async function cleanupSenderKeyMemory(
	authState: SignalAuthState,
	maxAge: number
): Promise<void> {
	try {
		const allMemory = await authState.keys.get('sender-key-memory', [])
		const now = Date.now()
		const cleanedMemory: { [jid: string]: any } = {}
		let hasChanges = false

		for (const [jid, memory] of Object.entries(allMemory)) {
			if (memory && typeof memory === 'object') {
				// For now, we keep all valid memory entries
				// In future, we could add timestamp tracking
				const validEntries: { [key: string]: boolean } = {}
				let hasValidEntries = false

				for (const [key, value] of Object.entries(memory)) {
					if (typeof value === 'boolean' && value === true) {
						validEntries[key] = value
						hasValidEntries = true
					} else {
						hasChanges = true
					}
				}

				if (hasValidEntries) {
					cleanedMemory[jid] = validEntries
				} else {
					hasChanges = true
				}
			} else if (memory !== null) {
				hasChanges = true
			}
		}

		if (hasChanges) {
			await authState.keys.set({ 'sender-key-memory': cleanedMemory })
		}
	} catch (error) {
		console.warn('Failed to cleanup sender key memory:', error)
	}
}

/**
 * Clean up corrupted sender keys
 */
async function cleanupCorruptedSenderKeys(
	authState: SignalAuthState,
	maxStatesPerGroup: number
): Promise<void> {
	try {
		const allSenderKeys = await authState.keys.get('sender-key', [])
		const keysToUpdate: { [key: string]: any } = {}
		let hasChanges = false

		for (const [keyId, keyData] of Object.entries(allSenderKeys)) {
			if (keyData) {
				try {
					// Try to parse the sender key data
					let parsed: any
					if (typeof keyData === 'string') {
						parsed = JSON.parse(keyData)
					} else if (keyData instanceof Uint8Array) {
						const str = Buffer.from(keyData).toString('utf-8')
						parsed = JSON.parse(str)
					} else {
						parsed = keyData
					}

					// Basic validation
					if (!Array.isArray(parsed) || parsed.length === 0) {
						// Invalid format, mark for deletion
						keysToUpdate[keyId] = null
						hasChanges = true
						continue
					}

					// Limit the number of states
					if (parsed.length > maxStatesPerGroup) {
						// Keep only the most recent states
						const trimmed = parsed.slice(-maxStatesPerGroup)
						keysToUpdate[keyId] = Buffer.from(JSON.stringify(trimmed), 'utf-8')
						hasChanges = true
					}

					// Validate each state
					const validStates = parsed.filter((state: any) => {
						return (
							state &&
							typeof state.senderKeyId === 'number' &&
							state.senderChainKey &&
							typeof state.senderChainKey.iteration === 'number' &&
							state.senderChainKey.seed &&
							state.senderSigningKey &&
							state.senderSigningKey.public
						)
					})

					if (validStates.length !== parsed.length) {
						if (validStates.length === 0) {
							// No valid states, delete the key
							keysToUpdate[keyId] = null
						} else {
							// Update with only valid states
							keysToUpdate[keyId] = Buffer.from(JSON.stringify(validStates), 'utf-8')
						}
						hasChanges = true
					}
				} catch (error) {
					// Corrupted key data, mark for deletion
					keysToUpdate[keyId] = null
					hasChanges = true
				}
			}
		}

		if (hasChanges) {
			await authState.keys.set({ 'sender-key': keysToUpdate })
		}
	} catch (error) {
		console.warn('Failed to cleanup corrupted sender keys:', error)
	}
}

/**
 * Clear sender key memory for a specific group
 */
export async function clearGroupSenderKeyMemory(
	authState: SignalAuthState,
	groupJid: string
): Promise<void> {
	try {
		await authState.keys.set({ 'sender-key-memory': { [groupJid]: null } })
	} catch (error) {
		console.warn('Failed to clear group sender key memory:', error)
	}
}

/**
 * Clear sender key for a specific group
 */
export async function clearGroupSenderKey(
	authState: SignalAuthState,
	groupJid: string,
	meId: string
): Promise<void> {
	try {
		// Create the sender key name format
		const senderKeyName = `${groupJid}::${meId}::0`
		await authState.keys.set({ 'sender-key': { [senderKeyName]: null } })
	} catch (error) {
		console.warn('Failed to clear group sender key:', error)
	}
}
