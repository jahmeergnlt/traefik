package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jahmeergnlt/traefik/pkg/server"
)

func main() {
	fmt.Println("Starting Traefik mock server...")
	s := server.NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)
	time.Sleep(100 * time.Millisecond)
	fmt.Println("Mock server started successfully.")
}
