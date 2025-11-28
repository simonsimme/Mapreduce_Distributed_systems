package mr

import (
	"bufio"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"6.5840/helpers"
	
)

const maxServerConcurrentUsers = 10

func StartServer(port string) {
	// Check command-line arguments
	
	addr := ":" + port

	log.Printf("Starting server on %s\n", addr)

	// Start listening on the specified port
	listener, err := net.Listen("tcp", addr)

	if err != nil {
		log.Fatalf("Failed to listen on %s: %v\n", addr, err)
	}

	// Close listener when done
	defer listener.Close()

	// Semaphore to limit concurrent users
	sem := make(chan struct{}, maxServerConcurrentUsers)

	// Main accept loop
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Accept error: %v\n", err)
			continue
		}

		// Acquire semaphore slot
		sem <- struct{}{}

		// Handle connection in a new goroutine
		go func(c net.Conn) {
			defer func() {
				c.Close()
				<-sem
			}()
			handleConnection(c)
		}(conn)
	}
}

func handleConnection(conn net.Conn) {
	// Read request from client
	reader := bufio.NewReader(conn)
	req, err := http.ReadRequest(reader)
	if err != nil {
		helpers.WriteBadRequest(conn)
		return
	}

	// Close request body when done
	defer req.Body.Close()

	// Handle based on method
	switch req.Method {
	case http.MethodGet:
		handleGET(conn, req)
	case http.MethodPost:
		handlePOST(conn, req)
	default:
		helpers.WriteNotImplemented(conn)
	}
}

// Handle GET requests
func handleGET(conn net.Conn, req *http.Request) {
	// Determine path
	path := req.URL.Path
	if path == "" {
		path = "/"
	}

	// Default file for "/"
	if path == "/" {
		path = "/index.html"
	}

	// Map URL path to filesystem path
	fsPath, err := pathToFile(path)
	if err != nil {
		helpers.WriteBadRequest(conn)
		return
	}

	// Check extension and get content type
	contentType, ok := helpers.GetContentType(fsPath)

	if !ok {
		// unsupported extension -> 400 Bad Request
		helpers.WriteBadRequest(conn)
		return
	}

	// Open file for reading
	f, err := os.Open(fsPath)

	log.Printf("requested: "+fsPath)

	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			helpers.WriteNotFound(conn)
		} else {
			// other errors -> 400 Bad Request as per spec
			helpers.WriteBadRequest(conn)
		}
		return
	}
	defer f.Close()

	// Read file content
	data, err := io.ReadAll(f)
	if err != nil {
		helpers.WriteBadRequest(conn)
		return
	}

	// Respond with file content
	helpers.WriteOK(conn, contentType, data)
}

// Handle POST requests
func handlePOST(conn net.Conn, req *http.Request) {
	// Read body data
	data, err := io.ReadAll(req.Body)
	if err != nil {
		helpers.WriteBadRequest(conn)
		return
	}

	log.Printf("Got POST %s (%d bytes)\n", req.URL.Path, len(data))

	log.Printf("Data: %s\n", string(data))

	// Map URL path to filesystem path
	fsPath, err := pathToFile(req.URL.Path)
	if err != nil {
		helpers.WriteBadRequest(conn)
		return
	}

	// Check extension
	_, ok := helpers.GetContentType(fsPath)
	if !ok {
		helpers.WriteBadRequest(conn)
		return
	}

	// Ensure directory exists
	dir := filepath.Dir(fsPath)
	err = os.MkdirAll(dir, 0755)
	if err != nil {
		helpers.WriteBadRequest(conn)
		return
	}

	// Create or append to file
	f, err := os.Create(fsPath)
	if err != nil {
		helpers.WriteBadRequest(conn)
		return
	}
	defer f.Close()

	// Write data to file
	_, err = f.Write(data)
	if err != nil {
		helpers.WriteBadRequest(conn)
		return
	}

	// Respond
	msg := []byte("File stored and available via GET " + req.URL.Path + "\n")
	helpers.WriteCreated(conn, "text/plain", msg)
}

// Map URL path to filesystem path with basic validation
func pathToFile(path string) (string, error) {
	// Basic validation to prevent directory traversal and invalid paths
	if strings.Contains(path, "..") || strings.Contains(path, "\\") || path == "" {
		return "", errors.New("invalid path")
	}

	// Map URL path to filesystem path
	fsPath := "content" + path
	return fsPath, nil
}
