package helpers

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
)

// Shared helper to write HTTP response
func WriteResponse(conn net.Conn, statusCode int, headers map[string]string, body []byte) error {

	// Prepare HTTP response
	resp := &http.Response{
		StatusCode:    statusCode,
		Status:        fmt.Sprintf("%d %s", statusCode, http.StatusText(statusCode)),
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        make(http.Header),
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
	}

	// Set headers
	for k, v := range headers {
		resp.Header.Set(k, v)
	}

	resp.Header.Set("Connection", "close")

	return resp.Write(conn)
}

// Helper for 200 OK response
func WriteOK(conn net.Conn, contentType string, body []byte) {
	_ = WriteResponse(conn, http.StatusOK, map[string]string{
		"Content-Type": contentType,
	}, body)
}

// Helper for 201 Created response
func WriteCreated(conn net.Conn, contentType string, body []byte) {
	_ = WriteResponse(conn, http.StatusCreated, map[string]string{
		"Content-Type": contentType,
	}, body)
}

// Helper for 400 Bad Request response
func WriteBadRequest(conn net.Conn) {
	body := []byte("<html><body><h1>400 Bad Request</h1></body></html>\n")
	_ = WriteResponse(conn, http.StatusBadRequest, map[string]string{
		"Content-Type": "text/html",
	}, body)
}

// Helper for 501 Not Implemented response
func WriteNotImplemented(conn net.Conn) {
	body := []byte("<html><body><h1>501 Not Implemented</h1></body></html>\n")
	_ = WriteResponse(conn, http.StatusNotImplemented, map[string]string{
		"Content-Type": "text/html",
	}, body)
}

// Helper for 404 Not Found response
func WriteNotFound(conn net.Conn) {
	body := []byte("<html><body><h1>404 Not Found</h1></body></html>\n")
	_ = WriteResponse(conn, http.StatusNotFound, map[string]string{
		"Content-Type": "text/html",
	}, body)
}

// Return content type based on file extension
func GetContentType(path string) (string, bool) {
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".html":
		return "text/html", true
	case ".txt":
		return "text/plain", true
	case ".gif":
		return "image/gif", true
	case ".jpeg", ".jpg":
		return "image/jpeg", true
	case ".css":
		return "text/css", true
	default:
		return "", false
	}
}
