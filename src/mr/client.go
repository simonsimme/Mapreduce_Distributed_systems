package mr

import (
	"io"
	"log"
	"net/http"
	"time"
	"bytes"
)

func Get_client(addr string, port string, file string) string {
	client := &http.Client{
		Timeout: time.Duration(10),
	}
	url := addr+port+"/"+file
	req, err := http.NewRequest("GET", url, nil)

	if(err != nil){
		log.Print("bad1")
	}
	resp, err := client.Do(req)

	if(err != nil){
		log.Print("bad2 %v",err)
	}

	defer resp.Body.Close()

    // Read the response body
    bodyBytes, err := io.ReadAll(resp.Body)

    if err != nil {
        log.Fatalf("Error reading body:", err) 
    }

    return string(bodyBytes)

}
func Post_client(addr string, port string, file string, content string) error{

	client := &http.Client{
		Timeout: time.Duration(10),
	}
	url := addr+port+"/"+file

	jsonBody := []byte(`{"client_message": "hello, server!"}`)
	bodyReader := bytes.NewReader(jsonBody)


	req, err := http.NewRequest("POST", url, bodyReader)

	if(err != nil){
		log.Print("bad1")
		 return err
	}
    
	resp, err := client.Do(req)

	if(err != nil){
		log.Print("bad2 %v",err)
		 return err
	}

	defer resp.Body.Close()

    return nil 

}