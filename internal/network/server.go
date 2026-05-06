package network

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"mesh-cu/internal/protocol"
)

type MessageHandler func(conn net.Conn, senderID string, name string, messageType protocol.MessageType, payload map[string]interface{}) error

type Server struct {
	Port     int
	NodeID   string
	Handler  MessageHandler
	listener net.Listener
}

func NewServer(nodeID string, port int, handler MessageHandler) *Server {
	return &Server{
		NodeID:  nodeID,
		Port:    port,
		Handler: handler,
	}
}

func (s *Server) Start(ctx context.Context) error {
	addr := fmt.Sprintf(":%d", s.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to start TCP server on port %d: %w", s.Port, err)
	}
	s.listener = listener
	log.Printf("[Network] TCP Server started on port %d", s.Port)

	go func() {
		defer s.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				conn, err := s.listener.Accept()
				if err != nil {
					continue
				}
				go s.handleConnection(conn)
			}
		}
	}()
	return nil
}

func (s *Server) Stop() {
	if s.listener != nil {
		s.listener.Close()
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	
	data, err := io.ReadAll(conn)
	if err != nil {
		return
	}

	if len(data) > 0 {
		header, payload, err := protocol.Decode(data)
		if err != nil {
			log.Printf("[Network Error] Failed to decode message: %v", err)
			return
		}

		if s.Handler != nil {
			senderName, _ := payload["sender_name"].(string)
			if senderName == "" {
				senderName = header.SenderID
			}

			err = s.Handler(conn, header.SenderID, senderName, header.MessageType, payload)
			if err != nil {
				log.Printf("[Network Error] Handler failed: %v", err)
			}
		}
	}
}

func SendMessage(peerIP string, peerPort int, message []byte) error {
	addr := net.JoinHostPort(peerIP, fmt.Sprintf("%d", peerPort))
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.Write(message)
	return err
}
