package main

import (
	"bufio"
	"fmt"
	"github.com/spf13/cobra"
	"net"
	"os"
	"strings"
)

type MedisClient struct {
	conn net.Conn
}

func NewMedisClient(addr string) (*MedisClient, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &MedisClient{conn: conn}, nil
}

func (client *MedisClient) runCommand(cmd string) (string, error) {
	_, err := client.conn.Write([]byte(cmd + "\r\n"))
	if err != nil {
		return "", err
	}
	resp := make([]byte, 1024)
	n, err := client.conn.Read(resp)
	if err != nil {
		return "", err
	}
	return string(resp[:n]), nil
}

func main() {
	fmt.Println("client")

	var host, port string

	var rootCmd = &cobra.Command{
		Use:   "medis-cli",
		Short: "A simple CLI for MiniRedis",
		RunE: func(cmd *cobra.Command, args []string) error {
			addr := net.JoinHostPort(host, port)
			client, err := NewMedisClient(addr)
			if err != nil {
				return err
			}
			defer func(conn net.Conn) {
				_ = conn.Close()
			}(client.conn)

			for {
				fmt.Print("medis> ")
				reader := bufio.NewReader(os.Stdin)
				cmdString, err := reader.ReadString('\n')
				if err != nil {
					return err
				}
				cmdString = strings.TrimSpace(cmdString)
				if cmdString == "exit" || cmdString == "quit" {
					return nil
				}
				resp, err := client.runCommand(cmdString)
				if err != nil {
					return err
				}
				fmt.Println(resp)
			}
		},
	}

	rootCmd.PersistentFlags().StringVarP(&host, "host", "H", "localhost", "Server host")
	rootCmd.PersistentFlags().StringVarP(&port, "port", "P", "6379", "Server port")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
