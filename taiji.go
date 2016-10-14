package taiji

import (
    "bufio"
    "io"
    "log"
    "net"
    "strconv"
    "strings"
    "syscall"
    "text/scanner"
    "github.com/lethe3000/taiji/backend/etcd"
)

// Server events
const (
    REGISTER = iota
    UNREGISTER
    DISCONNECT
    LIST_ALL
    BROADCAST
    SEND_TO
)

// Client only events
const (
    ECHO = iota
    CLOSE_CLIENT
)

type SendText struct {
    clientId string
    text string
}

type Event struct {
    eventType int
    data1 interface{}
    data2 interface{}
}

type Server struct {
    clientPort int
    controlPort int
    clients map[string] chan *Event
    masterChan chan *Event
    clientListener net.Listener
    controlListener net.Listener
    backend etcd.Backend
}

func NewServer(clientPort, controlPort int) (*Server) {
    return &Server{
        clientPort, controlPort,
        make(map[string] chan *Event),
        make(chan *Event, 32), nil, nil,
        &etcd.EtcdBackend{Urls: []string{"http://localhost:2379"}}}
}

func (m *Server) handleConnection(conn net.Conn) {
    reader := bufio.NewReader(conn)
    writer := bufio.NewWriter(conn)

    defer func() {
        writer.Flush()
        log.Printf("Closing connection for %s", conn.RemoteAddr());
        conn.Close()
    } ()

    helloLine, err := reader.ReadString('\n')
    if err != nil {
        log.Printf("Error reading: %s", err.Error())
        return
    }
    words := strings.Fields(helloLine)
    if len(words) != 2 || strings.ToUpper(words[0]) != "HELO" {
        writer.WriteString("Invalid HELO line\n")
        log.Printf("Invalid HELO line: %s", helloLine)
        return
    }

    clientId := strings.TrimSpace(words[1])
    clientChan := make(chan *Event, 10)
    m.masterChan <- &Event{REGISTER, clientId, clientChan}
    if m.backend.CheckClient(clientId) {
        for _, msg := range m.backend.Fetch(clientId) {
            log.Printf("Send message to back client[%s]: %s", clientId, msg)
            if !m.SendText(clientId, msg) {
                m.backend.Store(clientId, msg)
            }
        }
    }

    go func() {
        for {
            s, err := reader.ReadString('\n')
            if err != nil {
                if err != io.EOF {
                    log.Printf("Error reading: %s", err.Error())
                }
                log.Printf("DISCONNECT")
                clientChan <- &Event{CLOSE_CLIENT, 0, 0}
                m.masterChan <- &Event{DISCONNECT, clientId, 0}
                break
            } else {
                writer.WriteString(s)
                writer.Flush()
                if strings.TrimSpace(s) == "QUIT" {
                    log.Printf("QUIT")
                    clientChan <- &Event{CLOSE_CLIENT, 0, 0}
                    m.masterChan <- &Event{UNREGISTER, clientId, 0}
                    break
                }
            }
        }
    } ()

    for event := range clientChan {
        switch event.eventType {
        case BROADCAST:
            writer.WriteString("BROADCAST " + event.data1.(string) + "\n")
            writer.Flush()
        case SEND_TO:
            writer.WriteString("SEND_TO " + event.data1.(string) + "\n")
            writer.Flush()
        case CLOSE_CLIENT:
            return
        }
    }
}

func (m *Server) handleControlConnection(conn net.Conn) {
    reader := bufio.NewReader(conn)
    writer := bufio.NewWriter(conn)

    defer func() {
        writer.Flush()
        log.Printf("Closing control connection for %s", conn.RemoteAddr());
        conn.Close()
    } ()

    writer.WriteString("Welcome to PushServer console.\n")

    for {
        writer.WriteString("> ")
        writer.Flush()
        cmd, err := reader.ReadString('\n')
        if err != nil {
            if err != io.EOF {
                log.Printf("Error reading from control connection: %s", err.Error())
            }
            break
        }
        var s scanner.Scanner
        s.Init(strings.NewReader(cmd))
        s.Mode = scanner.ScanIdents | scanner.ScanInts | scanner.ScanStrings
        tok := s.Scan()
        if tok == scanner.Ident {
            switch s.TokenText() {
            case "l":
                result := make(chan []string)
                writer.WriteString("Listing clients:\n")
                m.masterChan <- &Event{LIST_ALL, writer, result}
                clients := <-result
                for _, clientId := range clients {
                    writer.WriteString("    " + clientId + "\n")
                }

            case "s":
                tok = s.Scan()
                if tok != scanner.Ident {
                    writer.WriteString("s <client id> <\"string to send\">\n")
                    break
                }
                clientId := s.TokenText()
                tok = s.Scan()
                if tok != scanner.String {
                    writer.WriteString("s <client id> <\"string to send\">\n")
                    break
                }
                if m.SendText(clientId, s.TokenText()) {
                    writer.WriteString("Message sent.\n")
                } else if !m.backend.CheckClient(clientId) {
                    writer.WriteString("Can't find " + clientId + "\n")
                }

            case "b":
                tok := s.Scan()
                if tok != scanner.String {
                    writer.WriteString("b <\"string to broadcast\">\n")
                    break
                }
                m.Broadcast(s.TokenText())
                writer.WriteString("Message broadcasted.\n")

            case "q":
                return

            default:
                writer.WriteString("Unrecognized command " + cmd + "\n")
            }

        } else {
            writer.WriteString("<cmd> [args]\n")
        }

        writer.Flush()
    }
}

func (s *Server) Broadcast(str string) {
    finish := make(chan bool)
    s.masterChan <- &Event{BROADCAST, str, finish}
    <-finish
}

func (s *Server) SendText(clientId string, str string) bool {
    finish := make(chan bool)
    s.masterChan <- &Event{SEND_TO, &SendText{clientId, str}, finish}
    return <-finish
}

func (m *Server) ListenAndServe() {
    go m.startControlListener()
    go m.startClientListener()

    m.backend.Init()

    for event := range m.masterChan {
        switch event.eventType {
        case REGISTER:
            clientId := event.data1.(string)
            log.Printf("Registering client %s", clientId)
            m.clients[clientId] = event.data2.(chan *Event)
            if m.backend.Register(clientId) != nil {
                log.Printf("Register error %s", clientId)
            }

        case UNREGISTER:
            clientId := event.data1.(string)
            log.Printf("Unregistering client %s", clientId)
            delete(m.clients, clientId)
            if m.backend.Deregister(clientId) != nil {
                log.Printf("Deregister error %s", clientId)
            }

        case DISCONNECT:
            clientId := event.data1.(string)
            log.Printf("Disconnect client %s", clientId)
            delete(m.clients, clientId)
            if m.backend.Disconnect(clientId) != nil {
                log.Printf("Disconnect error %s", clientId)
            }

        case LIST_ALL:
            log.Println("Listing all clients")
            var clientIds []string
            for clientId, _ := range m.clients {
                clientIds = append(clientIds, clientId)
            }
            event.data2.(chan []string) <- clientIds

        case BROADCAST:
            str := event.data1.(string)
            log.Printf("Broadcasting " + str + "...")
            for _, clientChan := range m.clients {
                // FIXME: may block!
                clientChan <- &Event{BROADCAST, str, 0}
            }
            event.data2.(chan bool) <- true

        case SEND_TO:
            clientId := event.data1.(*SendText).clientId
            clientChan, ok := m.clients[clientId]
            if ok {
                log.Println("Send message to " + clientId)
                // FIXME: may block!
                clientChan <- &Event{SEND_TO, event.data1.(*SendText).text, 0}
            } else {
                m.backend.Store(clientId, event.data1.(*SendText).text)
            }
            event.data2.(chan bool) <- ok

        }
    }
}

func (m *Server) close() {
    m.clientListener.Close()
    m.controlListener.Close()
}

func (m *Server) startClientListener() {
    var err error
    m.clientListener, err = net.Listen("tcp", ":" + strconv.Itoa((m.clientPort)))
    if err != nil {
        log.Printf("Error binding to client listening port: %s", err.Error())
    } else {
        for {
            conn, err := m.clientListener.Accept()
            if err != nil {
                log.Printf("Error accepting: %s", err.Error())
                if err == syscall.EINTR {
                    continue
                } else {
                    break
                }
            }
            go m.handleConnection(conn)
        }
    }
}

func (m *Server) startControlListener() {
    var err error
    m.controlListener, err = net.Listen("tcp", ":" + strconv.Itoa(m.controlPort))
    if err != nil {
        log.Printf("Error binding to control port: %s", err.Error())
    } else {
        for {
            conn, err := m.controlListener.Accept()
            if err != nil {
                log.Printf("Error accepting: %s", err.Error())
                if err == syscall.EINTR {
                    continue
                } else {
                    break
                }
            }
            go m.handleControlConnection(conn)
        }
    }
}

/*
func main() {
    server := &Server{ 5555, 6666, make(map[string] chan *Event), make(chan *Event, 32) }
    server.ListenAndServe()
}
*/

