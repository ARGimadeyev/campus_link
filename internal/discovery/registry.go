package discovery

import (
	"sync"
	"time"
)

type registeredPeer struct {
	Peer
	LastSeen time.Time
}

// PeerRegistry хранит список активных узлов в сети.
type PeerRegistry struct {
	mu    sync.RWMutex
	peers map[string]registeredPeer
}

// NewPeerRegistry создает новый экземпляр реестра и запускает очистку.
func NewPeerRegistry() *PeerRegistry {
	r := &PeerRegistry{
		peers: make(map[string]registeredPeer),
	}
	go r.cleanupLoop()
	return r
}

// Update добавляет или обновляет информацию об узле.
func (r *PeerRegistry) Update(peer Peer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.peers[peer.ID] = registeredPeer{
		Peer:     peer,
		LastSeen: time.Now(),
	}
}

// GetActivePeers возвращает список всех текущих активных узлов.
func (r *PeerRegistry) GetActivePeers() []Peer {
	r.mu.RLock()
	defer r.mu.RUnlock()

	active := make([]Peer, 0, len(r.peers))
	for _, p := range r.peers {
		active = append(active, p.Peer)
	}
	return active
}

// cleanupLoop раз в 10 секунд запускает очистку устаревших узлов.
func (r *PeerRegistry) cleanupLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C
		r.cleanup()
	}
}

func (r *PeerRegistry) cleanup() {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	for id, p := range r.peers {
		if now.Sub(p.LastSeen) > 30*time.Second {
			delete(r.peers, id)
		}
	}
}
