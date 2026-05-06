package protocol

import (
	"encoding/json"
	"fmt"
)

type MessageType string

const (
	TypePing         MessageType = "PING"
	TypePong         MessageType = "PONG"
	TypeChat         MessageType = "CHAT"
	TypeChatMessage  MessageType = "CHAT_MSG"
	TypeChatControl  MessageType = "CHAT_CTRL"
	TypeFileRequest  MessageType = "FILE_REQ"
	TypeFileAck      MessageType = "FILE_ACK"
	TypeFileAnnounce MessageType = "FILE_ANN"   // Анонс файла
	TypeChunkRequest MessageType = "CHUNK_REQ"  // Запрос чанка
	TypeFileChunk    MessageType = "FILE_CHUNK" // Сами данные чанка
)

type Header struct {
	MessageType MessageType `json:"type"`
	SenderID    string      `json:"sender_id"`
	SenderName  string      `json:"sender_name"`
	RecipientID string      `json:"recipient_id"`
}

type PingMessage struct {
	Header
	Timestamp int64 `json:"timestamp"`
}

type ChatMessage struct {
	Message string `json:"message"`
}

type ChatPayload struct {
	ChatID     string `json:"chat_id"`
	SenderNick string `json:"sender_nick"`
	Body       string `json:"body"`
	CreatedAt  int64  `json:"created_at"`
	MessageID  string `json:"message_id"`
}

// Структуры для полезной нагрузки CDN
type FileAnnouncePayload struct {
	FileID      string   `json:"file_id"`
	FileName    string   `json:"file_name"`
	FileSize    int64    `json:"file_size"`
	TotalChunks uint32   `json:"total_chunks"`
	Chunks      []uint32 `json:"chunks"` // Список имеющихся у пира кусков
}

type ChunkRequestPayload struct {
	FileID     string `json:"file_id"`
	ChunkIndex uint32 `json:"chunk_index"`
}

type ChunkPayload struct {
	FileID     string `json:"file_id"`
	ChunkIndex uint32 `json:"chunk_index"`
	Data       []byte `json:"data"`
}

func Encode(header Header, payload interface{}) ([]byte, error) {
	temp := struct {
		Header
		Payload interface{} `json:"payload"`
	}{
		Header:  header,
		Payload: payload,
	}
	return json.Marshal(temp)
}

func Decode(data []byte) (Header, map[string]interface{}, error) {
	var temp struct {
		Header
		Payload json.RawMessage `json:"payload"`
	}
	err := json.Unmarshal(data, &temp)
	if err != nil {
		return Header{}, nil, fmt.Errorf("failed to unmarshal message: %w", err)
	}

	var payloadMap map[string]interface{}
	// Безопасная проверка RawMessage
	if len(temp.Payload) > 0 && string(temp.Payload) != "null" {
		err = json.Unmarshal(temp.Payload, &payloadMap)
		if err != nil {
			return temp.Header, nil, fmt.Errorf("failed to unmarshal payload: %w", err)
		}
	}

	return temp.Header, payloadMap, nil
}
