package main

import (
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type Client struct {
	conn *websocket.Conn
	send chan []byte
}

type Room struct {
	clients map[*Client]bool
	mu      sync.Mutex
}

var (
	rooms   = make(map[string]*Room)
	roomsMu sync.Mutex
)

func handleConnections(w http.ResponseWriter, r *http.Request) {
	roomCode := r.URL.Query().Get("room")
	if roomCode == "" {
		http.Error(w, "Room code required", http.StatusBadRequest)
		return
	}

	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Upgrade error:", err)
		return
	}

	client := &Client{conn: ws, send: make(chan []byte, 256)}

	roomsMu.Lock()
	rm, exists := rooms[roomCode]
	if !exists {
		rm = &Room{clients: make(map[*Client]bool)}
		rooms[roomCode] = rm
	}
	roomsMu.Unlock()

	rm.mu.Lock()
	rm.clients[client] = true
	rm.mu.Unlock()

	defer func() {
		rm.mu.Lock()
		delete(rm.clients, client)
		if len(rm.clients) == 0 {
			roomsMu.Lock()
			delete(rooms, roomCode)
			roomsMu.Unlock()
		}
		rm.mu.Unlock()
		ws.Close()
	}()

	go func() {
		for msg := range client.send {
			if err := ws.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		}
	}()

	for {
		_, message, err := ws.ReadMessage()
		if err != nil {
			break
		}

		// Repassa a mensagem CRIPTOGRAFADA para todos na sala
		rm.mu.Lock()
		for c := range rm.clients {
			if c != client {
				select {
				case c.send <- message:
				default:
					close(c.send)
					delete(rm.clients, c)
				}
			}
		}
		rm.mu.Unlock()
	}
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	http.HandleFunc("/ws", handleConnections)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Relay E2EE ativo!"))
	})

	log.Printf("Servidor Relay rodando na porta %s...", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal(err)
	}
}
