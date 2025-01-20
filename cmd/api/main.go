package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"DETECT.go/internal/auth"
	"DETECT.go/internal/database"
	"github.com/gorilla/websocket"
)

type Message struct {
	Type    string `json:"type"`
	Payload string `json:"payload"`
}

const (
	TypeRegister      = "register"
	TypeLogin         = "login"
	TypePasswordReset = "password_reset"
	TypeVideoUpload   = "video_upload"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		// Allow all origins for development. Secure this for production.
		return true
	},
}

func handleWebSocketConnection(db database.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("Failed to upgrade to WebSocket: %v", err)
			return
		}
		defer conn.Close()

		log.Println("WebSocket connection established")

		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Printf("Error reading message: %v", err)
				break
			}

			var msg Message
			if err := json.Unmarshal(message, &msg); err != nil {
				conn.WriteMessage(websocket.TextMessage, []byte(`{"error":"invalid message format"}`))
				continue
			}

			switch msg.Type {
			case TypeRegister:
				handleRegister(conn, db, msg.Payload)
			case TypeLogin:
				handleLogin(conn, db, msg.Payload)
			case TypePasswordReset:
				handlePasswordReset(conn, db, msg.Payload)
			case TypeVideoUpload:
				handleVideoUpload(conn, msg.Payload)
			default:
				conn.WriteMessage(websocket.TextMessage, []byte(`{"error":"unknown message type"}`))
			}
		}
	}
}

func handleRegister(conn *websocket.Conn, db database.Service, payload string) {
	var data struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}

	if err := json.Unmarshal([]byte(payload), &data); err != nil {
		conn.WriteMessage(websocket.TextMessage, []byte(`{"error":"invalid payload format"}`))
		return
	}

	if exists, _ := db.UserExists(data.Email); exists {
		conn.WriteMessage(websocket.TextMessage, []byte(`{"error":"user already exists"}`))
		return
	}

	id, err := db.InsertUser(data.Email, data.Password)
	if err != nil {
		conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"error":"failed to register user: %v"}`, err)))
		return
	}

	response := fmt.Sprintf(`{"status":"success","message":"User registered successfully","user_id":%d}`, id)
	conn.WriteMessage(websocket.TextMessage, []byte(response))
}

func handleLogin(conn *websocket.Conn, db database.Service, payload string) {
	var data struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}

	if err := json.Unmarshal([]byte(payload), &data); err != nil {
		conn.WriteMessage(websocket.TextMessage, []byte(`{"error":"invalid payload format"}`))
		return
	}

	valid, err := db.VerifyUser(data.Email, data.Password)
	if err != nil || !valid {
		conn.WriteMessage(websocket.TextMessage, []byte(`{"error":"invalid credentials"}`))
		return
	}

	conn.WriteMessage(websocket.TextMessage, []byte(`{"status":"success","message":"Login successful"}`))
}

func handlePasswordReset(conn *websocket.Conn, db database.Service, payload string) {
	var data struct {
		Email       string `json:"email"`
		NewPassword string `json:"new_password"`
	}

	if err := json.Unmarshal([]byte(payload), &data); err != nil {
		conn.WriteMessage(websocket.TextMessage, []byte(`{"error":"invalid payload format"}`))
		return
	}

	if exists, _ := db.UserExists(data.Email); !exists {
		conn.WriteMessage(websocket.TextMessage, []byte(`{"error":"user does not exist"}`))
		return
	}

	_, err := db.InsertUser(data.Email, data.NewPassword) // Simulate password reset
	if err != nil {
		conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"error":"failed to reset password: %v"}`, err)))
		return
	}

	conn.WriteMessage(websocket.TextMessage, []byte(`{"status":"success","message":"Password reset successful"}`))
}

func handleVideoUpload(conn *websocket.Conn, payload string) {
	log.Println("Video upload not yet implemented")
	conn.WriteMessage(websocket.TextMessage, []byte(`{"error":"video upload not implemented"}`))
}

func gracefulShutdown(apiServer *http.Server, done chan bool) {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	<-ctx.Done()
	log.Println("shutting down gracefully")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := apiServer.Shutdown(ctx); err != nil {
		log.Printf("Server forced to shutdown: %v", err)
	}

	log.Println("Server exiting")
	done <- true
}

func main() {
	auth.NewAuth()
	db := database.New()
	defer db.Close()

	server := &http.Server{
		Addr:    ":8080",
		Handler: http.NewServeMux(),
	}

	http.HandleFunc("/ws", handleWebSocketConnection(db))

	done := make(chan bool, 1)
	go gracefulShutdown(server, done)

	log.Println("Starting server on :8080")
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("HTTP server error: %v", err)
	}

	<-done
	log.Println("Graceful shutdown complete.")
}
