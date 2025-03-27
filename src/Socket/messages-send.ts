import NodeCache from '@cacheable/node-cache'
import { Boom } from '@hapi/boom'
import { proto } from '../../WAProto'
import { DEFAULT_CACHE_TTLS, WA_DEFAULT_EPHEMERAL } from '../Defaults'
import { AnyMessageContent, MediaConnInfo, MessageReceiptType, MessageRelayOptions, MiscMessageGenerationOptions, SocketConfig, WAMessageKey } from '../Types'
import { aggregateMessageKeysNotFromMe, assertMediaContent, bindWaitForEvent, decryptMediaRetryData, encodeSignedDeviceIdentity, encodeWAMessage, encryptMediaRetryRequest, extractDeviceJids, generateMessageIDV2, generateWAMessage, getStatusCodeForMediaRetry, getUrlFromDirectPath, getWAUploadToServer, normalizeMessageContent, parseAndInjectE2ESessions, unixTimestampSeconds } from '../Utils'
import { getUrlInfo } from '../Utils/link-preview'
import { areJidsSameUser, BinaryNode, BinaryNodeAttributes, getBinaryNodeChild, getBinaryNodeChildren, isJidGroup, isJidUser, jidDecode, jidEncode, jidNormalizedUser, JidWithDevice, S_WHATSAPP_NET } from '../WABinary'
import { USyncQuery, USyncUser } from '../WAUSync'
import { makeGroupsSocket } from './groups'

// Constantes para melhorar a performance
const DEFAULT_PARTICIPANT_BLOCK_SIZE = 200
const MEDIA_CONN_CHECK_INTERVAL = 60000 // 1 minuto em ms

export const makeMessagesSocket = (config: SocketConfig) => {
	const {
		logger,
		linkPreviewImageThumbnailWidth,
		generateHighQualityLinkPreview,
		options: axiosOptions,
		patchMessageBeforeSending,
		cachedGroupMetadata,
	} = config
	const sock = makeGroupsSocket(config)
	const {
		ev,
		authState,
		processingMutex,
		signalRepository,
		upsertMessage,
		query,
		fetchPrivacySettings,
		sendNode,
		groupMetadata,
		groupToggleEphemeral,
	} = sock

	// Configurações para otimização
	const participantBlockSize = config.participantBlockSize || DEFAULT_PARTICIPANT_BLOCK_SIZE
	
	// Cache de dispositivos de usuários para reduzir queries
	const userDevicesCache = config.userDevicesCache || new NodeCache({
		stdTTL: DEFAULT_CACHE_TTLS.USER_DEVICES,
		useClones: false,
		checkperiod: 120 // Reduzir a frequência de verificação do cache
	})

	// Cache de tipos de mídia para evitar recálculo frequente
	const mediaTypeCache = new Map()
	
	// Cache para sessões verificadas para evitar verificações repetidas
	const verifiedSessionsCache = new Set()

	// Promessas em andamento para evitar duplicação de requisições
	const pendingMediaQueries = new Map()

	let mediaConn: Promise<MediaConnInfo>
	let lastMediaConnRefresh = 0
	
	// Função otimizada para atualizar a conexão de mídia
	const refreshMediaConn = async(forceGet = false) => {
		const now = Date.now()
		
		// Verifica se já existe uma consulta em andamento
		if(pendingMediaQueries.has('mediaConn')) {
			return pendingMediaQueries.get('mediaConn')
		}
		
		// Verifica se é necessário atualizar a conexão
		if(!mediaConn || forceGet || (now - lastMediaConnRefresh > MEDIA_CONN_CHECK_INTERVAL)) {
			const queryPromise = (async() => {
				try {
					const result = await query({
						tag: 'iq',
						attrs: {
							type: 'set',
							xmlns: 'w:m',
							to: S_WHATSAPP_NET,
						},
						content: [{ tag: 'media_conn', attrs: {} }]
					})
					
					const mediaConnNode = getBinaryNodeChild(result, 'media_conn')
					if(!mediaConnNode) {
						throw new Error('Media connection node not found')
					}
					
					const hosts = getBinaryNodeChildren(mediaConnNode, 'host')
					
					// Pré-processamento dos resultados para melhorar performance
					const node: MediaConnInfo = {
						hosts: hosts.map(({ attrs }) => ({
							hostname: attrs.hostname,
							maxContentLengthBytes: +attrs.maxContentLengthBytes,
						})),
						auth: mediaConnNode.attrs.auth,
						ttl: +mediaConnNode.attrs.ttl,
						fetchDate: new Date()
					}
					
					lastMediaConnRefresh = now
					logger.debug('fetched media conn')
					return node
				} finally {
					pendingMediaQueries.delete('mediaConn')
				}
			})()
			
			pendingMediaQueries.set('mediaConn', queryPromise)
			mediaConn = queryPromise
			return queryPromise
		}

		return mediaConn
	}

	/**
	 * Função otimizada para envio de recibos
	 */
	const sendReceipt = async(jid: string, participant: string | undefined, messageIds: string[], type: MessageReceiptType) => {
		if(!messageIds.length) {
			return // Evitar operações desnecessárias
		}
		
		const node: BinaryNode = {
			tag: 'receipt',
			attrs: {
				id: messageIds[0],
			},
		}
		
		// Atribuições condicionais otimizadas
		if(type === 'read' || type === 'read-self') {
			node.attrs.t = unixTimestampSeconds().toString()
		}

		if(type === 'sender' && isJidUser(jid)) {
			node.attrs.recipient = jid
			node.attrs.to = participant!
		} else {
			node.attrs.to = jid
			if(participant) {
				node.attrs.participant = participant
			}
		}

		if(type) {
			node.attrs.type = type
		}

		// Otimização para múltiplos IDs
		const remainingMessageIds = messageIds.length > 1 ? messageIds.slice(1) : null
		if(remainingMessageIds?.length) {
			// Pré-criar os itens para melhor performance
			const items = remainingMessageIds.map(id => ({
				tag: 'item',
				attrs: { id }
			}))
			
			node.content = [
				{
					tag: 'list',
					attrs: {},
					content: items
				}
			]
		}

		logger.debug({ attrs: node.attrs, messageCount: messageIds.length }, 'sending receipt for messages')
		await sendNode(node)
	}

	/**
	 * Envio otimizado de múltiplos recibos
	 */
	const sendReceipts = async(keys: WAMessageKey[], type: MessageReceiptType) => {
		if(!keys.length) return
		
		const recps = aggregateMessageKeysNotFromMe(keys)
		
		// Processamento paralelo para envio mais rápido
		const promises = recps.map(({ jid, participant, messageIds }) => 
			sendReceipt(jid, participant, messageIds, type)
		)
		
		await Promise.all(promises)
	}

	/**
	 * Leitura otimizada de mensagens
	 */
	const readMessages = async(keys: WAMessageKey[]) => {
		if(!keys.length) return
		
		// Carregar configurações uma única vez
		const privacySettings = await fetchPrivacySettings()
		const readType = privacySettings.readreceipts === 'all' ? 'read' : 'read-self'
		await sendReceipts(keys, readType)
	}

	/**
	 * Busca otimizada de dispositivos de usuários
	 */
	const getUSyncDevices = async(jids: string[], useCache: boolean, ignoreZeroDevices: boolean) => {
		if(!jids.length) return []
		
		// Otimização: remover duplicatas e processar de uma vez
		jids = [...new Set(jids)]
		
		const deviceResults: JidWithDevice[] = []
		const toFetch: string[] = []
		const userPromises: Promise<void>[] = []

		// Função para processar usuários individualmente a partir do cache
		const processUserFromCache = async(user: string, normalizedJid: string) => {
			const devices = userDevicesCache.get<JidWithDevice[]>(user)
			if(devices) {
				// Usar spread para manter a referência do array original
				deviceResults.push(...devices)
				logger.trace({ user }, 'using cache for devices')
			} else {
				toFetch.push(normalizedJid)
			}
		}

		// Processar jids em paralelo quando possível
		for(let jid of jids) {
			const user = jidDecode(jid)?.user
			if(!user) continue
			
			const normalizedJid = jidNormalizedUser(jid)
			
			if(useCache) {
				userPromises.push(processUserFromCache(user, normalizedJid))
			} else {
				toFetch.push(normalizedJid)
			}
		}
		
		// Aguardar processamento do cache
		if(userPromises.length) {
			await Promise.all(userPromises)
		}

		if(!toFetch.length) {
			return deviceResults
		}

		// Criar query otimizada
		const query = new USyncQuery()
			.withContext('message')
			.withDeviceProtocol()

		// Adicionar usuários em lote
		for(const jid of toFetch) {
			query.withUser(new USyncUser().withId(jid))
		}

		// Executar query
		const result = await sock.executeUSyncQuery(query)

		if(result) {
			const extracted = extractDeviceJids(result.list, authState.creds.me!.id, ignoreZeroDevices)
			const deviceMap: Record<string, JidWithDevice[]> = {}

			// Pré-processar os resultados para criar cache de uma vez
			for(const item of extracted) {
				deviceMap[item.user] = deviceMap[item.user] || []
				deviceMap[item.user].push(item)
				deviceResults.push(item)
			}

			// Atualizar cache em lote
			for(const key in deviceMap) {
				userDevicesCache.set(key, deviceMap[key])
			}
		}

		return deviceResults
	}

	/**
	 * Verificação otimizada de sessões
	 */
	const assertSessions = async(jids: string[], force: boolean) => {
		if(!jids.length) return false
		
		// Remover duplicatas para evitar operações desnecessárias
		jids = [...new Set(jids)]
		
		// Verificar sessões já verificadas recentemente
		if(!force) {
			jids = jids.filter(jid => !verifiedSessionsCache.has(jid))
			if(!jids.length) return false
		}
		
		let didFetchNewSession = false
		let jidsRequiringFetch: string[] = []
		
		if(force) {
			jidsRequiringFetch = jids
		} else {
			// Otimização: criar todos os endereços de uma vez
			const addrs = jids.map(jid => signalRepository.jidToSignalProtocolAddress(jid))
			const sessions = await authState.keys.get('session', addrs)
			
			// Filtrar apenas os JIDs que precisam de busca
			for(let i = 0; i < jids.length; i++) {
				const jid = jids[i]
				const signalId = addrs[i]
				if(!sessions[signalId]) {
					jidsRequiringFetch.push(jid)
				} else {
					// Adicionar ao cache para evitar verificação futura
					verifiedSessionsCache.add(jid)
				}
			}
		}

		if(jidsRequiringFetch.length) {
			logger.debug({ jidsRequiringFetch: jidsRequiringFetch.length }, 'fetching sessions')
			
			// Busca otimizada de sessões
			const result = await query({
				tag: 'iq',
				attrs: {
					xmlns: 'encrypt',
					type: 'get',
					to: S_WHATSAPP_NET,
				},
				content: [
					{
						tag: 'key',
						attrs: {},
						content: jidsRequiringFetch.map(
							jid => ({
								tag: 'user',
								attrs: { jid },
							})
						)
					}
				]
				})
			
			await parseAndInjectE2ESessions(result, signalRepository)
			
			// Adicionar os JIDs ao cache de sessões verificadas
			for(const jid of jidsRequiringFetch) {
				verifiedSessionsCache.add(jid)
			}

			didFetchNewSession = true
		}

		return didFetchNewSession
	}

	/**
	 * Função otimizada para envio de mensagens de operação de dados peer
	 */
	const sendPeerDataOperationMessage = async(
		pdoMessage: proto.Message.IPeerDataOperationRequestMessage
	): Promise<string> => {
		if(!authState.creds.me?.id) {
			throw new Boom('Not authenticated')
		}

		const protocolMessage: proto.IMessage = {
			protocolMessage: {
				peerDataOperationRequestMessage: pdoMessage,
				type: proto.Message.ProtocolMessage.Type.PEER_DATA_OPERATION_REQUEST_MESSAGE
			}
		}

		const meJid = jidNormalizedUser(authState.creds.me.id)

		return await relayMessage(meJid, protocolMessage, {
			additionalAttributes: {
				category: 'peer',
				push_priority: 'high_force',
			},
		})
	}

	/**
	 * Criação otimizada de nós de participantes
	 */
	const createParticipantNodes = async(
		jids: string[],
		message: proto.IMessage,
		extraAttrs?: BinaryNode['attrs']
	) => {
		if(!jids.length) {
			return { nodes: [], shouldIncludeDeviceIdentity: false }
		}
		
		const patched = await patchMessageBeforeSending(message, jids)
		const bytes = encodeWAMessage(patched)

		let shouldIncludeDeviceIdentity = false
		
		// Processamento paralelo para melhor performance
		const nodes = await Promise.all(
			jids.map(async jid => {
				const { type, ciphertext } = await signalRepository
					.encryptMessage({ jid, data: bytes })
				
				if(type === 'pkmsg') {
					shouldIncludeDeviceIdentity = true
				}

				// Criar nó otimizado
				return {
					tag: 'to',
					attrs: { jid },
					content: [{
						tag: 'enc',
						attrs: {
							v: '2',
							type,
							...(extraAttrs || {})
						},
						content: ciphertext
					}]
				}
			})
		)
		
		return { nodes, shouldIncludeDeviceIdentity }
	}

	/**
	 * Função de relay de mensagem altamente otimizada
	 */
	const relayMessage = async(
		jid: string,
		message: proto.IMessage,
		{ messageId: msgId, participant, additionalAttributes, additionalNodes, useUserDevicesCache, useCachedGroupMetadata, statusJidList }: MessageRelayOptions
	) => {
		// Inicialização de variáveis otimizada
		const meId = authState.creds.me!.id
		let shouldIncludeDeviceIdentity = false
		const { user, server } = jidDecode(jid)!
		const statusJid = 'status@broadcast'
		const isGroup = server === 'g.us'
		const isStatus = jid === statusJid
		const isLid = server === 'lid'

		// Configurações padrão otimizadas
		msgId = msgId || generateMessageIDV2(sock.user?.id)
		useUserDevicesCache = useUserDevicesCache !== false
		useCachedGroupMetadata = useCachedGroupMetadata !== false && !isStatus

		const destinationJid = (!isStatus) 
			? jidEncode(user, isLid ? 'lid' : isGroup ? 'g.us' : 's.whatsapp.net') 
			: statusJid
		
		const extraAttrs: Record<string, string> = {}

		// Função otimizada interna para envio a participantes
		async function sendToParticipants(devices: JidWithDevice[], senderKeyJids: string[]): Promise<string> {
			if(!devices.length) return msgId || generateMessageIDV2(sock.user?.id)
			
			const binaryNodeContent: BinaryNode[] = []
			const participants: BinaryNode[] = []

			const meMsg: proto.IMessage = {
				deviceSentMessage: {
					destinationJid,
					message
				}
			}

			await authState.keys.transaction(async() => {
				// Otimização: verificar tipo de mídia uma única vez e armazenar em cache
				let mediaTypeValue: string | undefined
				
				// Usar ID único baseado em propriedades da mensagem para cache
				const messageHash = JSON.stringify(Object.keys(message).sort())
				
				if(mediaTypeCache.has(messageHash)) {
					mediaTypeValue = mediaTypeCache.get(messageHash)
				} else {
					mediaTypeValue = getMediaType(message)
					if(mediaTypeValue) {
						mediaTypeCache.set(messageHash, mediaTypeValue)
					}
				}
				
				if(mediaTypeValue) {
					extraAttrs.mediatype = mediaTypeValue
				}

				if(normalizeMessageContent(message)?.pinInChatMessage) {
					extraAttrs['decrypt-fail'] = 'hide'
				}

				if(isGroup || isStatus) {
					// Buscar dados do grupo e mapa de chaves de remetente em paralelo
					const [groupData, senderKeyMap] = await Promise.all([
						(async() => {
							if(!isStatus && useCachedGroupMetadata && cachedGroupMetadata) {
								const data = await cachedGroupMetadata(jid)
								if(data && Array.isArray(data?.participants)) {
									logger.trace({ jid, participants: data.participants.length }, 'using cached group metadata')
									return data
								}
							}
							
							return isStatus ? undefined : groupMetadata(jid)
						})(),
						(async() => {
							if(!isStatus) {
								const result = await authState.keys.get('sender-key-memory', [jid])
								return result[jid] || {}
							}
							return {}
						})()
					])

					// Patches e processamento de mensagem otimizados
					const patched = await patchMessageBeforeSending(
						message, 
						devices.map(d => jidEncode(d.user, isLid ? 'lid' : 's.whatsapp.net', d.device))
					)
					
					const bytes = encodeWAMessage(patched)

					// Criptografia para mensagem em grupo
					const { ciphertext, senderKeyDistributionMessage } = await signalRepository.encryptGroupMessage({
						group: destinationJid,
						data: bytes,
						meId,
					})

					// Processar chaves de remetente necessárias
					if(senderKeyJids.length) {
						logger.debug({ senderKeyJids: senderKeyJids.length }, 'sending new sender key')

						const senderKeyMsg: proto.IMessage = {
							senderKeyDistributionMessage: {
								axolotlSenderKeyDistributionMessage: senderKeyDistributionMessage,
								groupId: destinationJid
							}
						}

						// Verificar sessões para os jids que precisam de chaves
						await assertSessions(senderKeyJids, false)

						// Criar nós para distribuição de chaves
						const result = await createParticipantNodes(senderKeyJids, senderKeyMsg, extraAttrs)
						shouldIncludeDeviceIdentity = shouldIncludeDeviceIdentity || result.shouldIncludeDeviceIdentity

						participants.push(...result.nodes)
					}

					// Adicionar conteúdo cifrado
					binaryNodeContent.push({
						tag: 'enc',
						attrs: { v: '2', type: 'skmsg' },
						content: ciphertext
					})

					// Atualizar mapa de chaves no storage
					await authState.keys.set({ 'sender-key-memory': { [jid]: senderKeyMap } })
				} else {
					// Processamento de mensagens diretas (não-grupo)
					const { user: meUser } = jidDecode(meId)!

					// Arrays otimizados para separar JIDs
					const allJids: string[] = []
					const meJids: string[] = []
					const otherJids: string[] = []
					
					// Classificação eficiente de JIDs
					for(const { user, device } of devices) {
						const isMe = user === meUser
						const deviceJid = jidEncode(
							isMe && isLid ? authState.creds?.me?.lid!.split(':')[0] || user : user, 
							isLid ? 'lid' : 's.whatsapp.net', 
							device
						)
						
						// Usar push para melhor performance que spread
						if(isMe) {
							meJids.push(deviceJid)
						} else {
							otherJids.push(deviceJid)
						}

						allJids.push(deviceJid)
					}

					// Verificar sessões para todos os JIDs de uma vez
					await assertSessions(allJids, false)

					// Criar nós em paralelo para melhor performance
					const nodePromises: Promise<{
						nodes: BinaryNode[],
						shouldIncludeDeviceIdentity: boolean
					}>[] = []
					
					if(meJids.length) {
						nodePromises.push(createParticipantNodes(meJids, meMsg, extraAttrs))
					}
					
					if(otherJids.length) {
						nodePromises.push(createParticipantNodes(otherJids, message, extraAttrs))
					}
					
					const results = await Promise.all(nodePromises)
					
					// Processar resultados
					for(const { nodes, shouldIncludeDeviceIdentity: s } of results) {
						participants.push(...nodes)
						shouldIncludeDeviceIdentity = shouldIncludeDeviceIdentity || s
					}
				}

				// Adicionar participantes ao conteúdo se existirem
				if(participants.length) {
					if(additionalAttributes?.category === 'peer') {
						const peerNode = participants[0]?.content?.[0] as BinaryNode
						if(peerNode) {
							binaryNodeContent.push(peerNode) // push only enc
						}
					} else {
						binaryNodeContent.push({
							tag: 'participants',
							attrs: {},
							content: participants
						})
					}
				}

				// Montar objeto de mensagem otimizado
				const stanza: BinaryNode = {
					tag: 'message',
					attrs: {
						id: msgId!,
						type: getMessageType(message),
						...(additionalAttributes || {})
					},
					content: binaryNodeContent
				}
				
				// Configuração de destino otimizada
				if(participant) {
					if(isJidGroup(destinationJid)) {
						stanza.attrs.to = destinationJid
						stanza.attrs.participant = participant.jid
					} else if(areJidsSameUser(participant.jid, meId)) {
						stanza.attrs.to = participant.jid
						stanza.attrs.recipient = destinationJid
					} else {
						stanza.attrs.to = participant.jid
					}
				} else {
					stanza.attrs.to = destinationJid
				}

				// Adicionar identidade do dispositivo se necessário
				if(shouldIncludeDeviceIdentity) {
					(stanza.content as BinaryNode[]).push({
						tag: 'device-identity',
						attrs: {},
						content: encodeSignedDeviceIdentity(authState.creds.account!, true)
					})

					logger.debug({ jid }, 'adding device identity')
				}

				// Adicionar nós adicionais se fornecidos
				if(additionalNodes?.length) {
					(stanza.content as BinaryNode[]).push(...additionalNodes)
				}

				logger.debug({ msgId, deviceCount: participants.length }, `sending message to devices`)

				// Enviar nó
				await sendNode(stanza)
			})

			return msgId || generateMessageIDV2(sock.user?.id)
		}

		// Tratar participante específico se fornecido
		if(participant) {
			if(!isGroup && !isStatus) {
				additionalAttributes = { 
					...(additionalAttributes || {}), 
					'device_fanout': 'false' 
				}
			}

			const { user, device } = jidDecode(participant.jid)!
			const devices: JidWithDevice[] = [{ user, device }]
			
			return await sendToParticipants(devices, [])
		}

		// Lógica otimizada para mensagens em grupo
		if(isGroup || isStatus) {
			// Obter lista de participantes de forma otimizada
			let participantsList: string[] = []
			
			if(isGroup) {
				try {
					const groupData = useCachedGroupMetadata && cachedGroupMetadata 
						? await cachedGroupMetadata(jid) 
						: await groupMetadata(jid)
						
					if(groupData?.participants?.length) {
						// Otimização: mapear IDs diretamente sem spread
						participantsList = groupData.participants.map(p => p.id)
						logger.debug({ jid, participantCount: participantsList.length }, 'got group participants')
					}
				} catch(error) {
					logger.warn({ jid, error }, 'error getting group metadata')
				}
			}
			
			// Adicionar JIDs de status se aplicável
			if(isStatus && statusJidList?.length) {
				participantsList.push(...statusJidList)
			}

			// Otimização para grupos grandes: divisão em blocos
			if(participantsList.length > participantBlockSize) {
				logger.debug({ 
					msgId, 
					totalParticipants: participantsList.length, 
					blockSize: participantBlockSize 
				}, `splitting group message into ${Math.ceil(participantsList.length/participantBlockSize)} blocks`)

				// Dividir participantes em blocos de forma eficiente
				const participantBlocks: string[][] = []
				for(let i = 0; i < participantsList.length; i += participantBlockSize) {
					participantBlocks.push(participantsList.slice(i, i + participantBlockSize))
				}

				// Obter mapa de chaves de remetente uma única vez
				const senderKeyMap = isStatus 
					? {} 
					: (await authState.keys.get('sender-key-memory', [jid]))[jid] || {}

				// Processar blocos em paralelo para melhor performance
				await Promise.all(participantBlocks.map(async(blockParticipants, blockIndex) => {
					// Obter dispositivos para este bloco
					const additionalDevices = await getUSyncDevices(blockParticipants, !!useUserDevicesCache, false)
					const blockSenderKeyJids: string[] = []
					
					// Verificar quais dispositivos precisam de novas chaves
					for(const { user, device } of additionalDevices) {
						const deviceJid = jidEncode(user, isLid ? 'lid' : 's.whatsapp.net', device)
						if(!senderKeyMap[deviceJid]) {
							blockSenderKeyJids.push(deviceJid)
							senderKeyMap[deviceJid] = true
						}
					}
					
					logger.debug({ 
						msgId, 
						blockIndex,
						blockSize: blockParticipants.length, 
						deviceCount: additionalDevices.length,
						newKeyCount: blockSenderKeyJids.length 
					}, `sending to block`)
					
					await sendToParticipants(additionalDevices, blockSenderKeyJids)
				}))
				
				// Atualizar o mapa de chaves no storage após processamento de todos os blocos
				if(!isStatus) {
					await authState.keys.set({ 'sender-key-memory': { [jid]: senderKeyMap } })
				}
				
				return msgId
			} else {
				// Para grupos menores, usar lógica simplificada
				const additionalDevices = await getUSyncDevices(participantsList, !!useUserDevicesCache, false)
				const senderKeyJids: string[] = []
				
				if(!isStatus) {
					const senderKeyMap = (await authState.keys.get('sender-key-memory', [jid]))[jid] || {}
					
					// Otimização em loop único
					for(const { user, device } of additionalDevices) {
						const deviceJid = jidEncode(user, isLid ? 'lid' : 's.whatsapp.net', device)
						if(!senderKeyMap[deviceJid]) {
							senderKeyJids.push(deviceJid)
							senderKeyMap[deviceJid] = true
						}
					}
					
					// Salvar as mudanças no mapa de chaves se houver novas chaves
					if(senderKeyJids.length) {
						await authState.keys.set({ 'sender-key-memory': { [jid]: senderKeyMap } })
					}
				}
				
				return await sendToParticipants(additionalDevices, senderKeyJids)
			}
		} else {
			// Otimização para mensagens diretas (não-grupo)
			const { user: meUser } = jidDecode(meId)!
			const devices: JidWithDevice[] = [{ user }]
			
			if(user !== meUser) {
				devices.push({ user: meUser })
			}

			// Adicionar dispositivos extras se necessário
			if(additionalAttributes?.['category'] !== 'peer') {
				const additionalDevices = await getUSyncDevices([ meId, jid ], !!useUserDevicesCache, true)
				devices.push(...additionalDevices)
			}
			
			return await sendToParticipants(devices, [])
		}
	}

	/**
	 * Função otimizada para determinar o tipo de mensagem
	 */
	const getMessageType = (message: proto.IMessage) => {
		// Verificar tipos específicos primeiro para otimização
		if(message.pollCreationMessage || message.pollCreationMessageV2 || message.pollCreationMessageV3) {
			return 'poll'
		}
		return 'text'
	}

	/**
	 * Função otimizada para determinar o tipo de mídia
	 */
	const getMediaType = (message: proto.IMessage) => {
		// Verificação de tipos em ordem de probabilidade para otimização
		if(message.imageMessage) return 'image'
		if(message.videoMessage) return message.videoMessage.gifPlayback ? 'gif' : 'video'
		if(message.audioMessage) return message.audioMessage.ptt ? 'ptt' : 'audio'
		if(message.documentMessage) return 'document'
		if(message.stickerMessage) return 'sticker'
		if(message.contactMessage) return 'vcard'
		if(message.contactsArrayMessage) return 'contact_array'
		if(message.liveLocationMessage) return 'livelocation'
		if(message.listMessage) return 'list'
		if(message.listResponseMessage) return 'list_response'
		if(message.buttonsResponseMessage) return 'buttons_response'
		if(message.orderMessage) return 'order'
		if(message.productMessage) return 'product'
		if(message.interactiveResponseMessage) return 'native_flow_response'
		if(message.groupInviteMessage) return 'url'
	}

	/**
	 * Função otimizada para obter tokens de privacidade
	 */
	const getPrivacyTokens = async(jids: string[]) => {
		if(!jids.length) return
		
		// Timestamp único para todos os tokens
		const t = unixTimestampSeconds().toString()
		
		// Criar tokens em um único loop
		const tokens = jids.map(jid => ({
			tag: 'token',
			attrs: {
				jid: jidNormalizedUser(jid),
				t,
				type: 'trusted_contact'
			}
		}))
		
		return await query({
			tag: 'iq',
			attrs: {
				to: S_WHATSAPP_NET,
				type: 'set',
				xmlns: 'privacy'
			},
			content: [
				{
					tag: 'tokens',
					attrs: {},
					content: tokens
				}
			]
		})
	}

	// Funções utilitárias otimizadas
	const waUploadToServer = getWAUploadToServer(config, refreshMediaConn)
	const waitForMsgMediaUpdate = bindWaitForEvent(ev, 'messages.media-update')

	// Limpar caches periodicamente
	setInterval(() => {
		// Limitar tamanho do cache de mídia
		if(mediaTypeCache.size > 500) {
			mediaTypeCache.clear()
		}
		
		// Limitar tamanho do cache de sessões verificadas
		if(verifiedSessionsCache.size > 1000) {
			verifiedSessionsCache.clear()
		}
	}, 3600000) // Limpar a cada hora

	return {
		...sock,
		getPrivacyTokens,
		assertSessions,
		relayMessage,
		sendReceipt,
		sendReceipts,
		readMessages,
		refreshMediaConn,
		waUploadToServer,
		fetchPrivacySettings,
		sendPeerDataOperationMessage,
		createParticipantNodes,
		getUSyncDevices,
		
		/**
		 * Função otimizada para atualização de mensagens de mídia
		 */
		updateMediaMessage: async(message: proto.IWebMessageInfo) => {
			if(!message?.key?.id) {
				throw new Boom('Message ID is required')
			}
			
			const content = assertMediaContent(message.message)
			const mediaKey = content.mediaKey!
			const meId = authState.creds.me!.id
			const node = await encryptMediaRetryRequest(message.key, mediaKey, meId)

			let error: Error | undefined = undefined
			
			// Execução paralela otimizada
			await Promise.all([
				sendNode(node),
				waitForMsgMediaUpdate(async(update) => {
					// Busca otimizada pelo ID correspondente
					const result = update.find(c => c.key.id === message.key.id)
					if(result) {
						if(result.error) {
							error = result.error
						} else {
							try {
								const media = await decryptMediaRetryData(result.media!, mediaKey, result.key.id!)
								
								// Verificar resultado da operação
								if(media.result !== proto.MediaRetryNotification.ResultType.SUCCESS) {
									const resultStr = proto.MediaRetryNotification.ResultType[media.result]
									throw new Boom(
										`Media re-upload failed by device (${resultStr})`,
										{ data: media, statusCode: getStatusCodeForMediaRetry(media.result) || 404 }
									)
								}

								// Atualização eficiente de propriedades
								content.directPath = media.directPath
								content.url = getUrlFromDirectPath(content.directPath)

								logger.debug({ directPath: media.directPath, key: result.key }, 'media update successful')
							} catch(err) {
								error = err
							}
						}

						return true
					}
				})
			])

			if(error) {
				throw error
			}

			// Notificar atualização
			ev.emit('messages.update', [
				{ key: message.key, update: { message: message.message } }
			])

			return message
		},
		
		/**
		 * Função otimizada para envio de mensagens
		 */
		sendMessage: async(
			jid: string,
			content: AnyMessageContent,
			options: MiscMessageGenerationOptions = {}
		) => {
			const userJid = authState.creds.me!.id
			
			// Tratamento especial para mensagens efêmeras
			if(
				typeof content === 'object' &&
				'disappearingMessagesInChat' in content &&
				typeof content['disappearingMessagesInChat'] !== 'undefined' &&
				isJidGroup(jid)
			) {
				const { disappearingMessagesInChat } = content
				const value = typeof disappearingMessagesInChat === 'boolean' ?
					(disappearingMessagesInChat ? WA_DEFAULT_EPHEMERAL : 0) :
					disappearingMessagesInChat
				
				return await groupToggleEphemeral(jid, value)
			} else {
				// Geração otimizada de mensagem
				const messageId = options.messageId || generateMessageIDV2(sock.user?.id)
				
				const fullMsg = await generateWAMessage(
					jid,
					content,
					{
						logger,
						userJid,
						getUrlInfo: text => getUrlInfo(
							text,
							{
								thumbnailWidth: linkPreviewImageThumbnailWidth,
								fetchOpts: {
									timeout: 3_000,
									...axiosOptions || {}
								},
								logger,
								uploadImage: generateHighQualityLinkPreview
									? waUploadToServer
									: undefined
							},
						),
						getProfilePicUrl: sock.profilePictureUrl,
						upload: waUploadToServer,
						mediaCache: config.mediaCache,
						options: config.options,
						messageId,
						...options,
					}
				)
				
				// Preparar atributos adicionais baseados no tipo de mensagem
				const additionalAttributes: BinaryNodeAttributes = {}
				const additionalNodes: BinaryNode[] = []
				
				// Detectar tipo de mensagem para configurações específicas
				if('delete' in content && !!content.delete) {
					// Mensagem de exclusão
					if(isJidGroup(content.delete?.remoteJid as string) && !content.delete?.fromMe) {
						additionalAttributes.edit = '8' // admin delete
					} else {
						additionalAttributes.edit = '7' // self delete
					}
				} else if('edit' in content && !!content.edit) {
					additionalAttributes.edit = '1' // edit message
				} else if('pin' in content && !!content.pin) {
					additionalAttributes.edit = '2' // pin message
				} else if('poll' in content && !!content.poll) {
					// Adicionar metadados para enquete
					additionalNodes.push({
						tag: 'meta',
						attrs: {
							polltype: 'creation'
						},
					} as BinaryNode)
				}

				// Aviso para opção descontinuada
				if('cachedGroupMetadata' in options) {
					console.warn('cachedGroupMetadata in sendMessage are deprecated, now cachedGroupMetadata is part of the socket config.')
				}

				// Enviar mensagem com relay otimizado
				await relayMessage(
					jid, 
					fullMsg.message!, 
					{ 
						messageId: fullMsg.key.id!, 
						useCachedGroupMetadata: options.useCachedGroupMetadata, 
						additionalAttributes, 
						statusJidList: options.statusJidList, 
						additionalNodes 
					}
				)
				
				// Emitir evento próprio se configurado
				if(config.emitOwnEvents) {
					process.nextTick(() => {
						processingMutex.mutex(() => (
							upsertMessage(fullMsg, 'append')
						))
					})
				}

				return fullMsg
			}
		}
	}
}
