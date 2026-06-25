// Package main provides the CLI entrypoint for aegora.
package main

import "fmt"

func startupMessage() string {
	return "Aegora starting..."
}

func main() {
	fmt.Println(startupMessage())
}
