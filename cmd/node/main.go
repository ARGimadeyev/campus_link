package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"time"

	"mesh-cu/internal/cdn"
	"mesh-cu/internal/discovery"
	"mesh-cu/internal/network"
	"mesh-cu/internal/protocol"
	"mesh-cu/internal/storage"
)

type createGroupCommand struct {
	Name  string
	Parts []string
}

func parseCreateGroupCommand(line string) (createGroupCommand, error) {
	var cmd createGroupCommand
	fields, err := splitCLIArgs(line)
	if err != nil {
		return cmd, err
	}
	if len(fields) == 0 || fields[0] != "/create" {
		return cmd, errors.New("unsupported command")
	}
	for i := 1; i < len(fields); i++ {
		switch fields[i] {
		case "-name":
			i++
			if i >= len(fields) {
				return cmd, errors.New("missing value for -name")
			}
			cmd.Name = strings.TrimSpace(fields[i])
		case "-parts":
			i++
			if i >= len(fields) {
				return cmd, errors.New("missing value for -parts")
			}
			partsValue := fields[i]
			for i+1 < len(fields) && !strings.HasPrefix(fields[i+1], "-") {
				i++
				partsValue += strings.TrimSpace(fields[i])
			}
			partsValue = strings.Trim(partsValue, "[]")
			cmd.Parts = strings.Split(partsValue, ",")
		default:
			return cmd, fmt.Errorf("unknown flag: %s", fields[i])
		}
	}
	if cmd.Name == "" {
		return cmd, errors.New("group name is required")
	}
	if len(cmd.Parts) == 0 {
		return cmd, errors.New("participants are required")
	}
	return cmd, nil
}

func splitCLIArgs(in string) ([]string, error) {
	var out []string
	var b strings.Builder
	inQuote := false
	for _, r := range in {
		switch {
		case r == '"':
			inQuote = !inQuote
		case (r == ' ' || r == '\t') && !inQuote:
			if b.Len() > 0 {
				out = append(out, b.String())
				b.Reset()
			}
		default:
			b.WriteRune(r)
		}
	}
	if inQuote {
		return nil, errors.New("unclosed quote")
	}
	if b.Len() > 0 {
		out = append(out, b.String())
	}
	return out, nil
}

func normalizeParticipants(raw []string, self string) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0, len(raw)+1)
	for _, p := range raw {
		p = strings.TrimSpace(p)
		if p == "" || p == self {
			continue
		}
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	return out
}

func printCommandUsage(command string, usage string, example string) {
	fmt.Printf("\r[ERROR] Usage: %s\n[ERROR] Example: %s\nyou: ", usage, example)
}

func main() {
	name := flag.String("name", "", "Name of the node")
	port := flag.Int("port", 8080, "TCP port to listen on")
	storageDir := flag.String("storage", "./storage", "Directory to store files")
	flag.Parse()

	// Генерируем уникальное имя узла, если не указано явно
	if *name == "" {
		hostname, err := os.Hostname()
		if err != nil {
			*name = fmt.Sprintf("node-%d", os.Getpid())
		} else {
			*name = fmt.Sprintf("node-%s-%d", hostname, os.Getpid())
		}
	}

	fm, err := cdn.NewFileManager(*storageDir)
	if err != nil {
		log.Fatalf("Failed to initialize FileManager: %v", err)
	}
	cdnMgr := cdn.NewCDNManager(*name, fm)
	registry := discovery.NewPeerRegistry()

	db, err := storage.OpenSQLite("")
	if err != nil {
		log.Fatalf("Failed to open sqlite: %v", err)
	}
	defer db.Close()
	if err := storage.Migrate(db); err != nil {
		log.Fatalf("Failed to migrate sqlite: %v", err)
	}
	chatRepo := storage.NewChatRepo(db)
	msgRepo := storage.NewMessageRepo(db)

	type chatType string
	const (
		chatTypePrivate chatType = "private"
		chatTypeGroup   chatType = "group"
	)

	var sessionMu sync.RWMutex
	activeChatID := ""
	activeChatType := chatTypePrivate
	knownNickToPeerID := make(map[string]string)
	groupMembers := make(map[string]map[string]struct{})

	// Welcome Message
	fmt.Printf("Welcome to Mesh-CU Messenger, %s!\n", *name)
	fmt.Println("Listening for peers and messages...")
	fmt.Println("Type your message and press Enter to send.")
	fmt.Println("Chat commands: /chat [nick-name]|group:[chat-id], /new, /all, /create -name [group name] -parts [nick1,nick2,...]")
	fmt.Println("-----------------------------------------")

	fmt.Printf("Starting node %s on port %d...\n", *name, *port)

	// Handler для входящих сообщений
	handleIncomingMessage := func(conn net.Conn, senderID string, senderName string, messageType protocol.MessageType, payload map[string]interface{}) error {
		switch messageType {
		case protocol.TypeChat, protocol.TypeChatMessage:
			message, ok := payload["body"].(string)
			if !ok {
				message, ok = payload["message"].(string)
			}
			if !ok {
				log.Printf("[Handler Error] Invalid chat message format from %s", senderID)
				return fmt.Errorf("invalid chat message format")
			}
			chatID, _ := payload["chat_id"].(string)
			if chatID == "" {
				chatID = "global"
			}
			senderNick, _ := payload["sender_nick"].(string)
			if senderNick == "" {
				senderNick = senderName
			}
			fmt.Printf("\r[%s] %s: %s\nyou:  ", chatID, senderNick, message)
			_ = chatRepo.EnsureChat(chatID, "group", chatID, time.Now().Unix())
			_ = chatRepo.UpsertParticipant(chatID, senderNick, senderID)
			_ = msgRepo.SaveMessage(fmt.Sprintf("%d", time.Now().UnixNano()), chatID, senderNick, message, time.Now().Unix(), time.Now().Unix())
			if strings.HasPrefix(chatID, "group:") {
				sessionMu.Lock()
				members, exists := groupMembers[chatID]
				if !exists {
					members = make(map[string]struct{})
					groupMembers[chatID] = members
				}
				members[senderID] = struct{}{}
				sessionMu.Unlock()
			}
		case protocol.TypeChatControl:
			action, _ := payload["action"].(string)
			if action != "group_sync" {
				return nil
			}
			chatID, _ := payload["chat_id"].(string)
			groupName, _ := payload["group_name"].(string)
			creator, _ := payload["creator"].(string)
			rawParts, _ := payload["participants"].([]interface{})
			participants := make([]string, 0, len(rawParts))
			for _, rp := range rawParts {
				if nick, ok := rp.(string); ok && strings.TrimSpace(nick) != "" {
					participants = append(participants, strings.TrimSpace(nick))
				}
			}
			_ = chatRepo.EnsureChat(chatID, "group", groupName, time.Now().Unix())
			for _, nick := range participants {
				_ = chatRepo.UpsertParticipant(chatID, nick, nick)
			}
			_ = chatRepo.UpsertParticipant(chatID, creator, creator)
			sessionMu.Lock()
			members := make(map[string]struct{})
			for _, nick := range participants {
				if peerID, ok := knownNickToPeerID[nick]; ok {
					members[peerID] = struct{}{}
				}
			}
			if creatorID, ok := knownNickToPeerID[creator]; ok {
				members[creatorID] = struct{}{}
			}
			members[*name] = struct{}{}
			groupMembers[chatID] = members
			sessionMu.Unlock()
			fmt.Printf("\r[SYSTEM]: Group synced: %s (%s) by %s\nyou:  ", groupName, chatID, creator)
		case protocol.TypeFileAnnounce:
			// 1. Извлекаем данные из мапы
			fID, _ := payload["file_id"].(string) // Если ID это строка, используй .(string)
			fName, _ := payload["file_name"].(string)

			if fID == "" {
				fID, _ = payload["FileID"].(string)
			}
			if fName == "" {
				fName, _ = payload["FileName"].(string)
			}
			// 2. Чтобы cdnMgr «увидел» файл, нужно собрать структуру и вызвать HandleAnnounce
			// Превращаем мапу обратно в типизированную нагрузку для CDN[cite: 1, 5]
			var p protocol.FileAnnouncePayload
			p.FileID = fID
			p.FileName = fName
			// Получаем остальные поля (размер и чанки), если они есть в payload
			if size, ok := payload["file_size"].(float64); ok {
				p.FileSize = int64(size)
			}

			var totalChunks uint32
			if tc, ok := payload["total_chunks"].(float64); ok {
				totalChunks = uint32(tc)
			} else {
				totalChunks = uint32((p.FileSize + cdn.ChunkSize - 1) / cdn.ChunkSize)
			}

			// 3. ПЕРЕДАЕМ В МЕНЕДЖЕР (теперь fID используется внутри, ошибка уйдет)[cite: 2]
			cdnMgr.HandleAnnounce(p, senderID)

			cdnMgr.Lock() // Важно: используй Lock, а не RLock
			if _, exists := cdnMgr.Files[fID]; !exists {
				cdnMgr.Files[fID] = &cdn.FileInfo{
					ID:          fID,
					Name:        fName,
					Size:        p.FileSize,
					TotalChunks: totalChunks,
					OwnedChunks: make(map[uint32]bool),
				}
			}
			cdnMgr.Unlock()

			fmt.Printf("\r[CDN]: Peer %s has file: %s (ID: %s)\nyou:  ", senderName, fName, fID)
		case protocol.TypeFileRequest:
			fileID, _ := payload["file_id"].(string)
			idx, _ := payload["chunk_index"].(float64)

			fi, ok := cdnMgr.Files[fileID]
			if ok {
				var data []byte
				var err error
				if fi.OriginalPath != "" {
					data, err = fm.ReadChunkFromPath(fi.OriginalPath, uint32(idx))
				} else {
					data, err = fm.ReadChunk(fi.Name, uint32(idx))
				}

				if err != nil || len(data) == 0 {
					log.Printf("[ERROR] Chunk %d not found for file %s", int(idx), fileID)
					return nil
				}

				// ВАЖНО: SenderID должен быть ВАШИМ (Kamil)
				respHeader := protocol.Header{
					MessageType: protocol.TypeFileChunk,
					SenderID:    *name, // Используйте переменную имени текущего узла
					SenderName:  *name,
				}

				respPayload := map[string]interface{}{
					"file_id":     fileID,
					"chunk_index": idx,
					"data":        data,
				}

				encoded, _ := protocol.Encode(respHeader, respPayload)

				// Отправляем конкретно тому, кто просил (senderID)
				for _, peer := range registry.GetActivePeers() {
					if peer.ID == senderID {
						// fmt.Printf("\r[DEBUG]: Sending chunk %v to %s at %s:%d\n", idx, peer.ID, peer.IP, peer.Port)

						network.SendMessage(peer.IP, peer.Port, encoded)
						// log.Printf("[CDN] Sent chunk %d to %s", int(idx), senderID)
						break
					}
				}
			}

		case protocol.TypeFileChunk:
			// Нам прилетел кусок файла
			fileID, _ := payload["file_id"].(string)
			idx, _ := payload["chunk_index"].(float64)

			var data []byte
			if strData, ok := payload["data"].(string); ok {
				data, _ = base64.StdEncoding.DecodeString(strData)
			} else if byteData, ok := payload["data"].([]byte); ok {
				data = byteData
			} else {
				log.Printf("[ERROR] Payload 'data' is missing or not a string! Type is: %T", payload["data"])
			}

			fi, ok := cdnMgr.Files[fileID]
			if ok {
				// Используем правильный путь для записи чанка
				err := fm.WriteChunk(fi.Name, uint32(idx), data)
				if err != nil {
					log.Printf("[ERROR] Failed to write chunk %d for file %s: %v", int(idx), fi.Name, err)
				} else {
					cdnMgr.Lock()
					fi.OwnedChunks[uint32(idx)] = true
					cdnMgr.Unlock()
					// fmt.Printf("\r[CDN]: Received chunk %d for %s\nyou:  ", int(idx), fi.Name)
				}
			}

			// Внутри case protocol.TypeFileChunk в handleIncomingMessage - добавлено отсутствие ошибки при недоступности fi
			if ok && uint32(idx)+1 < uint32(fi.TotalChunks) {
				// Формируем такой же запрос (TypeFileRequest), но для idx + 1
				// И отправляем его обратно senderID
			} else if ok {
				fmt.Printf("\n[CDN]: File %s download complete!\nyou: ", fi.Name)
			}
		case protocol.TypePing:
			// Можно обработать пинг, если нужно, но для мессенджера пока игнорируем
		case protocol.TypePong:
			// Можно обработать понг, если нужно
		default:
			log.Printf("[Handler] Received unknown message type %s from %s", messageType, senderID)
		}
		return nil
	}

	// // Инициализируем реестр пиров
	// registry := discovery.NewPeerRegistry()

	// Создаем сервис обнаружения
	discService := discovery.NewDiscoveryService(*name, *port)

	// Канал для найденных пиров
	peerChan := make(chan discovery.Peer)
	ctx, cancel := context.WithCancel(context.Background())

	// Запускаем сервис обнаружения (вещание + прослушивание) в фоне
	go discService.Start(ctx, peerChan)

	// Запускаем TCP сервер для сообщений
	server := network.NewServer(*name, *port, handleIncomingMessage)
	go func() {
		if err := server.Start(ctx); err != nil {
			log.Fatalf("[Network] Failed to start server: %v", err)
		}
	}()

	time.Sleep(1 * time.Second)

	// Слушаем найденных соседей и обновляем реестр с уведомлением
	go func() {
		knownPeers := make(map[string]string) // Track known peers to announce new ones
		for {
			select {
			case <-ctx.Done():
				return
			case peer, ok := <-peerChan:
				if !ok {
					return
				}
				// Анонс нового пира
				if _, exists := knownPeers[peer.ID]; !exists {
					fmt.Printf("\r[SYSTEM]: New peer joined: %s (%s:%d)\nyou: ", peer.Name, peer.IP, peer.Port)
					knownPeers[peer.ID] = peer.Name
				}
				sessionMu.Lock()
				knownNickToPeerID[peer.Name] = peer.ID
				sessionMu.Unlock()
				registry.Update(peer)

			case <-time.After(2 * time.Second):
				// Проверка на "пропавших" без отдельной горутины снаружи
				activePeers := registry.GetActivePeers()
				activeMap := make(map[string]bool)
				for _, p := range activePeers {
					activeMap[p.ID] = true
				}

				for id, name := range knownPeers {
					if !activeMap[id] {
						fmt.Printf("\r[SYSTEM]: %s has left the network\nyou:  ", name)
						sessionMu.Lock()
						delete(knownNickToPeerID, name)
						sessionMu.Unlock()
						delete(knownPeers, id)
					}
				}
			}
		}

	}()

	// Периодически выводим список активных узлов для наглядности (можно убрать в финальной версии)
	// go func() {
	// 	ticker := time.NewTicker(5 * time.Second)
	// 	defer ticker.Stop()
	// 	for {
	// 		select {
	// 		case <-ctx.Done():
	// 			return
	// 		case <-ticker.C:
	// 			active := registry.GetActivePeers()
	// 			// This part is mostly for debugging. In a real messenger, you might not want to spam this.
	// 			if len(active) > 0 {
	// 				// fmt.Printf("\n--- Active Peers (%d) ---\n", len(active))
	// 				// for _, p := range active {
	// 				// 	fmt.Printf("- %s (%s:%d)\n", p.Name, p.IP, p.Port)
	// 				// }
	// 				// fmt.Println("------------------------")
	// 			}
	// 		}
	// 	}
	// }()

	// Цикл чтения из консоли и рассылки сообщений
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		fmt.Print("you: ")
		for scanner.Scan() {
			line := scanner.Text()
			if len(line) == 0 {
				fmt.Print("you: ")
				continue
			}
			if strings.HasPrefix(line, "/") {
				parts := strings.Fields(line)
				if parts[0] == "/chat" {
					if len(parts) < 2 {
						printCommandUsage("/chat", "/chat [nick-name]|group:[chat-id]", "/chat alice")
						continue
					}
					target := parts[1]
					sessionMu.Lock()
					if strings.HasPrefix(target, "group:") {
						groupToken := strings.TrimPrefix(target, "group:")
						groupToken = strings.TrimSpace(groupToken)
						groupToken = strings.Trim(groupToken, "[]")
						target = "group:" + groupToken
						activeChatType = chatTypeGroup
						activeChatID = target
						if activeChatID == "group:" {
							sessionMu.Unlock()
							printCommandUsage("/chat", "/chat [nick-name]|group:[chat-id]", "/chat group:group-123")
							continue
						}
						members, exists := groupMembers[activeChatID]
						if !exists {
							members = make(map[string]struct{})
							groupMembers[activeChatID] = members
						}
						members[*name] = struct{}{}
						sessionMu.Unlock()
						_ = chatRepo.EnsureChat(activeChatID, "group", activeChatID, time.Now().Unix())
						_ = chatRepo.UpsertParticipant(activeChatID, *name, *name)
						_ = msgRepo.MarkReadUpToLatest(activeChatID, *name, time.Now().Unix())
						fmt.Printf("\r[SYSTEM]: Switched to group chat '%s'\nyou: ", activeChatID)
					} else {
						peerID, ok := knownNickToPeerID[target]
						if !ok {
							sessionMu.Unlock()
							fmt.Printf("\r[ERROR]: unknown user '%s'\nyou: ", target)
							continue
						}
						activeChatType = chatTypePrivate
						activeChatID = peerID
						sessionMu.Unlock()
						_ = chatRepo.EnsureChat(activeChatID, "private", target, time.Now().Unix())
						_ = chatRepo.UpsertParticipant(activeChatID, target, peerID)
						_ = chatRepo.UpsertParticipant(activeChatID, *name, *name)
						_ = msgRepo.MarkReadUpToLatest(activeChatID, *name, time.Now().Unix())
						fmt.Printf("\r[SYSTEM]: Switched to private chat with %s\nyou: ", target)
					}
					continue
				}
				if strings.HasPrefix(line, "/create") {
					cmd, err := parseCreateGroupCommand(line)
					if err != nil {
						fmt.Printf("\r[ERROR]: %v\n", err)
						printCommandUsage("/create", "/create -name [group name] -parts [nick1,nick2,...]", "/create -name Team -parts alice,bob")
						continue
					}
					normalized := normalizeParticipants(cmd.Parts, *name)
					if len(normalized) < 2 {
						fmt.Printf("\r[ERROR]: group must include at least 2 participants excluding you\n")
						printCommandUsage("/create", "/create -name [group name] -parts [nick1,nick2,...]", "/create -name Team -parts alice,bob")
						continue
					}
					unknown := make([]string, 0)
					memberPeerIDs := make(map[string]struct{})
					sessionMu.RLock()
					for _, nick := range normalized {
						peerID, ok := knownNickToPeerID[nick]
						if !ok {
							unknown = append(unknown, nick)
							continue
						}
						memberPeerIDs[peerID] = struct{}{}
					}
					sessionMu.RUnlock()
					if len(unknown) > 0 {
						fmt.Printf("\r[ERROR]: unknown users: %s\n", strings.Join(unknown, ", "))
						printCommandUsage("/create", "/create -name [group name] -parts [nick1,nick2,...]", "/create -name Team -parts alice,bob")
						continue
					}
					chatID := "group:" + strconv.FormatInt(time.Now().UnixNano(), 10)
					_ = chatRepo.EnsureChat(chatID, "group", cmd.Name, time.Now().Unix())
					_ = chatRepo.UpsertParticipant(chatID, *name, *name)
					for _, nick := range normalized {
						_ = chatRepo.UpsertParticipant(chatID, nick, nick)
					}
					sessionMu.Lock()
					memberPeerIDs[*name] = struct{}{}
					groupMembers[chatID] = memberPeerIDs
					activeChatType = chatTypeGroup
					activeChatID = chatID
					sessionMu.Unlock()

					header := protocol.Header{MessageType: protocol.TypeChatControl, SenderID: *name, SenderName: *name}
					payload := map[string]interface{}{"action": "group_sync", "chat_id": chatID, "group_name": cmd.Name, "participants": normalized, "creator": *name}
					encoded, _ := protocol.Encode(header, payload)
					for _, peer := range registry.GetActivePeers() {
						if _, ok := memberPeerIDs[peer.ID]; !ok || peer.ID == *name {
							continue
						}
						_ = network.SendMessage(peer.IP, peer.Port, encoded)
					}
					fmt.Printf("\r[SYSTEM]: Group created '%s' (%s)\nyou: ", cmd.Name, chatID)
					continue
				}
				if parts[0] == "/new" {
					if len(parts) != 1 {
						printCommandUsage("/new", "/new", "/new")
						continue
					}
					chats, err := chatRepo.ListAllWithUnread(*name, true)
					if err != nil {
						fmt.Printf("\r[ERROR]: %v\nyou: ", err)
						continue
					}
					for _, c := range chats {
						chatKind := "private"
						if strings.HasPrefix(c.ChatID, "group:") {
							chatKind = "group"
						}
						fmt.Printf("\r[CHAT] [%s] %s (%s) unread=%d\n", chatKind, c.Name, c.ChatID, c.Unread)
					}
					fmt.Print("you: ")
					continue
				}
				if parts[0] == "/all" {
					if len(parts) != 1 {
						printCommandUsage("/all", "/all", "/all")
						continue
					}
					chats, err := chatRepo.ListAllWithUnread(*name, false)
					if err != nil {
						fmt.Printf("\r[ERROR]: %v\nyou: ", err)
						continue
					}
					for _, c := range chats {
						chatKind := "private"
						if strings.HasPrefix(c.ChatID, "group:") {
							chatKind = "group"
						}
						fmt.Printf("\r[CHAT] [%s] %s (%s) unread=%d\n", chatKind, c.Name, c.ChatID, c.Unread)
					}
					fmt.Print("you: ")
					continue
				}
				if parts[0] == "/announce" {
					if len(parts) < 2 {
						fmt.Printf("\r[ERROR]: Usage: /announce <path> [optional_id]\nyou: ")
						continue
					}
					path := parts[1]
					var fID string
					if len(parts) > 2 {
						fID = parts[2]
					} else {
						// Простейшая генерация короткого ID на базе времени
						fID = fmt.Sprintf("%x", time.Now().UnixNano())[:6]
					}
					info, err := os.Stat(path)
					if err != nil {
						fmt.Printf("\r[ERROR]: File not found: %s\nyou: ", path)
						continue
					}

					// 1. Регистрируем файл в локальном менеджере (чтобы мы знали, что раздаем)
					fi := cdnMgr.RegisterLocalFile(fID, info.Name(), info.Size(), path)

					// 2. Создаем заголовок сообщения
					header := protocol.Header{
						MessageType: protocol.TypeFileAnnounce, // Должно быть "FILE_ANN" из твоего types.go
						SenderID:    *name,
						SenderName:  *name,
					}

					// 3. Формируем полезную нагрузку (важно: ключи должны совпадать с обработчиком!)
					payload := map[string]interface{}{
						"file_id":      fID,
						"file_name":    info.Name(),
						"file_size":    info.Size(), // Передаем чистые байты (int64)
						"total_chunks": fi.TotalChunks,
					}

					// log.Printf("Size of file %d, and name %s, and total chunks %d", info.Size(), info.Name(), fi.TotalChunks)
					// log.Printf("Size: %.2f MB", float64(info.Size())/1000000)

					// 4. Кодируем в JSON/байты
					encoded, err := protocol.Encode(header, payload)
					if err != nil {
						fmt.Printf("\r[ERROR]: Failed to encode: %v\nyou: ", err)
						continue
					}

					// 5. РАССЫЛКА: отправляем всем, кого нашли через Discovery
					activePeers := registry.GetActivePeers()
					count := 0
					for _, peer := range activePeers {
						if peer.ID != *name {
							network.SendMessage(peer.IP, peer.Port, encoded)
							count++
						}
					}

					fmt.Printf("\r[SYSTEM]: Announced file %s to %d peers\nyou: ", path, count)
				}
				if parts[0] == "/download" && len(parts) > 1 {
					fileID := parts[1]

					cdnMgr.RLock()
					fi, exists := cdnMgr.Files[fileID]
					cdnMgr.RUnlock()

					if !exists {
						fmt.Printf("\r[ERROR]: File ID %s unknown. Wait for announce.\nyou: ", fileID)
						continue
					}
					fmt.Printf("\r[SYSTEM]: Starting download for %s (%d chunks)...\nyou: ", fi.Name, fi.TotalChunks)

					activePeers := registry.GetActivePeers()
					if len(activePeers) == 0 {
						fmt.Printf("\r[ERROR]: No active peers to request chunks from.\nyou: ")
						continue
					}

					for chunkIdx := uint32(0); chunkIdx < fi.TotalChunks; chunkIdx++ {
						header := protocol.Header{
							MessageType: protocol.TypeFileRequest,
							SenderID:    *name,
							SenderName:  *name,
						}

						payload := map[string]interface{}{
							"file_id":     fileID,
							"chunk_index": float64(chunkIdx),
						}

						encoded, err := protocol.Encode(header, payload)
						if err != nil {
							fmt.Printf("\r[ERROR]: Failed to encode chunk %d request: %v\nyou: ", chunkIdx, err)
							continue
						}

						for _, peer := range activePeers {
							if peer.ID != *name {
								network.SendMessage(peer.IP, peer.Port, encoded)
							}
						}
					}
					fmt.Printf("\r[CDN]: Requested all %d chunks from network\nyou: ", fi.TotalChunks)
				}

				fmt.Print("you: ")
				continue
			}

			// fmt.Printf("you: %s\n", line)
			// Упаковываем сообщение
			header := protocol.Header{
				MessageType: protocol.TypeChatMessage,
				SenderID:    *name,
				SenderName:  *name,
			}
			sessionMu.RLock()
			currentChatID := activeChatID
			currentChatType := activeChatType
			sessionMu.RUnlock()
			if currentChatID == "" {
				fmt.Println("\r[SYSTEM]: chat is not selected. Use /chat <nick-name> or /chat group:<id>")
				fmt.Print("you: ")
				continue
			}
			header.RecipientID = currentChatID
			messageID := fmt.Sprintf("%d", time.Now().UnixNano())
			chatPayload := map[string]interface{}{
				"chat_id":     currentChatID,
				"sender_nick": *name,
				"body":        line,
				"created_at":  time.Now().Unix(),
				"message_id":  messageID,
			}
			encodedMsg, err := protocol.Encode(header, chatPayload)
			if err != nil {
				log.Printf("[Encoder Error] Failed to encode message: %v", err)
				fmt.Print("you: ")
				continue
			}

			// Отправка в активный чат
			activePeers := registry.GetActivePeers()
			if len(activePeers) == 0 {
				fmt.Println("\r[SYSTEM]: delivery failed: no active peers.")
			}
			delivered := 0
			for _, peer := range activePeers {
				if peer.ID == *name {
					continue
				}
				if currentChatType == chatTypePrivate && peer.ID != currentChatID {
					continue
				}
				if currentChatType == chatTypeGroup {
					sessionMu.RLock()
					members := groupMembers[currentChatID]
					_, ok := members[peer.ID]
					sessionMu.RUnlock()
					if !ok {
						continue
					}
				}
				err := network.SendMessage(peer.IP, peer.Port, encodedMsg)
				if err != nil {
					log.Printf("[Network Error] Failed to send message to %s (%s:%d): %v", peer.Name, peer.IP, peer.Port, err)
					continue
				}
				delivered++
			}
			_ = chatRepo.EnsureChat(currentChatID, string(currentChatType), currentChatID, time.Now().Unix())
			_ = chatRepo.UpsertParticipant(currentChatID, *name, *name)
			_ = msgRepo.SaveMessage(messageID, currentChatID, *name, line, time.Now().Unix(), time.Now().Unix())
			if delivered == 0 {
				fmt.Printf("\r[SYSTEM]: delivery failed for chat '%s'\n", currentChatID)
			}
			fmt.Print("you: ")
		}
		if err := scanner.Err(); err != nil {
			log.Printf("[UI Error] Reading standard input: %v", err)
		}
	}()

	// Ждем сигнала прерывания (Ctrl+C)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig

	log.Println("Shutting down...")
	cancel()      // Отменяем контекст, чтобы остановить горутины
	server.Stop() // Останавливаем TCP сервер
}
