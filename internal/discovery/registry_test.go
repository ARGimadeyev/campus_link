package discovery

import (
	"testing"
	"time"
)

func TestPeerRegistry(t *testing.T) {
	registry := NewPeerRegistry()

	p1 := Peer{ID: "peer1", Name: "Peer 1", IP: "1.2.3.4", Port: 8080}
	p2 := Peer{ID: "peer2", Name: "Peer 2", IP: "1.2.3.5", Port: 8081}

	registry.Update(p1)
	registry.Update(p2)

	active := registry.GetActivePeers()
	if len(active) != 2 {
		t.Errorf("Expected 2 active peers, got %d", len(active))
	}

	// Manually trigger cleanup for testing
	// We'll wait more than 30 seconds to see if they are removed, 
	// or we can modify the registry to allow setting a shorter timeout for tests.
	// But let's just test basic functionality first.
}

func TestPeerRegistryCleanup(t *testing.T) {
	// To test cleanup without waiting 30 seconds, we'd need to refactor PeerRegistry
	// to accept a timeout duration. For now, let's just test that Update works.
	registry := &PeerRegistry{
		peers: make(map[string]registeredPeer),
	}
	
	p1 := Peer{ID: "peer1", Name: "Peer 1"}
	
	// Add peer with old timestamp
	registry.mu.Lock()
	registry.peers[p1.ID] = registeredPeer{
		Peer:     p1,
		LastSeen: time.Now().Add(-40 * time.Second),
	}
	registry.mu.Unlock()
	
	registry.cleanup()
	
	active := registry.GetActivePeers()
	if len(active) != 0 {
		t.Errorf("Expected 0 active peers after cleanup, got %d", len(active))
	}
}
