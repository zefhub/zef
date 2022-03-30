package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

type Message struct {
	Text    string `json:"text"`
	Channel string `json:"channel"`
}

func main() {
	// Create mattermost message
	msg := Message{
		Channel: os.Getenv("CHANNEL"),
		Text: fmt.Sprintf(
			"**New ZefDB release available: %s.**\n**%s**\nChangelog: %s\n\nDownload: %s",
			os.Getenv("NAME"),
			strings.Split(os.Getenv("GITHUB_REF"), "/")[2],
			os.Getenv("DESCRIPTION"),
			fmt.Sprintf("http://%s", os.Getenv("S3_PATH")),
		),
	}

	message, err := json.Marshal(&msg)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("--------------")
	fmt.Println(string(message))
	fmt.Println("--------------")

	// Send message to mattermost
	req, err := http.NewRequest("POST", os.Getenv("MATTERMOST_URL"), bytes.NewBuffer(message))
	req.Header.Set("Content-Type", "application/json")
	if err != nil {
		log.Fatal(err)
	}

	client := &http.Client{
		Timeout: 30 * time.Second,
	}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(string(body))
}
