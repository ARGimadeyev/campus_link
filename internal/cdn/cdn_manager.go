package cdn

import (
	"sync"

	"mesh-cu/internal/protocol"
)

type FileInfo struct {
	ID           string
	Name         string
	OriginalPath string
	Size         int64
	TotalChunks  uint32
	OwnedChunks  map[uint32]bool
}

type CDNManager struct {
	fm        *FileManager
	mu        sync.RWMutex
	Files     map[string]*FileInfo           // My files (including partially downloaded)
	PeerFiles map[string]map[string][]uint32 // fileID -> peerID -> chunkIndices
	NodeID    string
}

func NewCDNManager(nodeID string, fm *FileManager) *CDNManager {
	return &CDNManager{
		fm:        fm,
		Files:     make(map[string]*FileInfo),
		PeerFiles: make(map[string]map[string][]uint32),
		NodeID:    nodeID,
	}
}

func (cm *CDNManager) Lock() {
	cm.mu.Lock()
}

func (cm *CDNManager) Unlock() {
	cm.mu.Unlock()
}

func (cm *CDNManager) RLock() {
	cm.mu.RLock()
}

func (cm *CDNManager) RUnlock() {
	cm.mu.RUnlock()
}

func (cm *CDNManager) HandleAnnounce(payload protocol.FileAnnouncePayload, senderID string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if _, ok := cm.PeerFiles[payload.FileID]; !ok {
		cm.PeerFiles[payload.FileID] = make(map[string][]uint32)
	}
	cm.PeerFiles[payload.FileID][senderID] = payload.Chunks
}

func (cm *CDNManager) GetChunkOwners(fileID string, chunkIndex uint32) []string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	var owners []string
	if peers, ok := cm.PeerFiles[fileID]; ok {
		for peerID, chunks := range peers {
			for _, idx := range chunks {
				if idx == chunkIndex {
					owners = append(owners, peerID)
					break
				}
			}
		}
	}
	return owners
}

func (cm *CDNManager) RegisterLocalFile(fileID, name string, size int64, originalPath string) *FileInfo {
	totalChunks := uint32((size + ChunkSize - 1) / ChunkSize)
	owned := make(map[uint32]bool)
	for i := uint32(0); i < totalChunks; i++ {
		owned[i] = true
	}

	fi := &FileInfo{
		ID:           fileID,
		Name:         name,
		OriginalPath: originalPath,
		Size:         size,
		TotalChunks:  totalChunks,
		OwnedChunks:  owned,
	}

	cm.mu.Lock()
	cm.Files[fileID] = fi
	cm.mu.Unlock()

	return fi
}

func (cm *CDNManager) GetOwnedChunks(fileID string) []uint32 {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	fi, ok := cm.Files[fileID]
	if !ok {
		return nil
	}

	var chunks []uint32
	for idx, owned := range fi.OwnedChunks {
		if owned {
			chunks = append(chunks, idx)
		}
	}
	return chunks
}
