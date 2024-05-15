package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

var data = make(map[string]string)

const Host = "127.0.0.1"
const Protocol = "tcp"
const SleepDuration = 10000 * time.Millisecond

type Dost struct {
	port string
	conn *net.Conn
}

var dosts = make(map[string]Dost)

func logData() {
	for {
		time.Sleep(SleepDuration)
		log.Println("data is: ", data)
	}
}

func main() {
	// run main.go <port> <dPort> <d1>
	args := os.Args[1:]
	listener, err := net.Listen(Protocol, net.JoinHostPort(Host, args[0]))
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Listening Dunia ki commands on :", listener.Addr())

	clusterListener, err := net.Listen(Protocol, net.JoinHostPort(Host, args[1]))
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Listening Dost ki commands on :", clusterListener.Addr())

	establishConnectionToDost(args[2:])
	go handleConnections(&listener)
	go handleConnections(&clusterListener)
	logData()
}

func handleConnections(listener *net.Listener) {
	for {
		conn, err := (*listener).Accept()
		if err != nil {
			log.Println(err)
		}
		handleConn(&conn)
	}
}

func establishConnectionToDost(args []string) {
	log.Println("Connecting to dosts...")
	isConnected := false
	for !isConnected {
		isConnected = true
		for _, port := range args {
			if _, ok := dosts[port]; !ok {
				time.Sleep(2 * time.Second)
				conn, err := net.Dial(Protocol, net.JoinHostPort(Host, port))
				if err != nil {
					log.Println(err)
					isConnected = false
				} else {
					log.Println("Connected to dost:", conn.RemoteAddr())
					dosts[port] = Dost{
						port: port,
						conn: &conn,
					}
				}
			}
		}
	}
	log.Println("Connected to all dosts!!!", dosts)
}

func handleConn(conn *net.Conn) {
	log.Println("New connection from :", (*conn).RemoteAddr())
	for {
		err := handleRead(conn)
		if err != nil {
			log.Println(err)
			return
		}
	}
}

func handleRead(conn *net.Conn) error {
	data := make([]byte, 64)
	_, err := (*conn).Read(data)
	if err != nil {
		return err
	}
	log.Println("Command Received: ", string(data))
	input := strings.Split(string(data), "\n")[0]
	command := strings.Split(input, " ")
	var result string
	switch command[0] {
	case "GET":
		result, err = handleGet(command[1])
		break
	case "SET":
		result, err = handleSet(command[1], command[2], true)
		break
	case "DSET":
		result, err = handleSet(command[1], command[2], false)
		break
	default:
		log.Println("Unknown Command Received: ", string(data))
	}
	if err != nil {
		log.Println(err)
	}
	_, err = (*conn).Write([]byte(result))
	if err != nil {
		return err
	}
	return nil
}

func handleSet(key string, val string, b bool) (string, error) {
	data[key] = val
	if b {
		go propagateSet(key, val)
	}
	return "", nil
}

// function to propagate this change to data of each server
func propagateSet(key string, val string) {
	command := "DSET " + key + " " + val
	for _, dost := range dosts {
		_, err := (*dost.conn).Write([]byte(command))
		if err != nil {
			log.Println("Failed to propagate: [", command, "] to dost: ", dost.port)
		}
	}
}

func handleGet(key string) (string, error) {
	log.Println("Processing Get Key: ", key)
	if val, ok := data[key]; ok {
		log.Println("Value is: ", val)
		return val, nil
	}
	return fmt.Sprintf("%v Key not found", key), errors.New(fmt.Sprintf("%v key not found", key))
}
