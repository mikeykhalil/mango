package proxy

import (
	"fmt"
	"net"
	"regexp"
	"runtime"
)

type UDPProxy struct {
	Config
	laddr   *net.UDPAddr
	backend *net.UDPAddr
	in      chan []byte
	conn *net.UDPConn
}

type Config struct {
	DogStatsDTagInjectors func(host net.Addr) []string
	DogStatsDTagNameTransformer map[string]string
	DogStatsDTagFilterPattern   string
}

func NewUDPProxy(network, laddr, backend string, confFunc ...func(*Config)) (*UDPProxy, error) {
	c := Config{}
	for _, f := range confFunc {
		f(&c)
	}
	local, err := net.ResolveUDPAddr("udp", laddr)
	if err != nil {
		return nil, fmt.Errorf("could not parse local address for udp server: %w", err)
	}
	remote, err := net.ResolveUDPAddr("udp", backend)
	if err != nil {
		return nil, fmt.Errorf("could not parse remote address for udp server: %w", err)
	}
	return &UDPProxy{c, local, remote, make(chan []byte, 100), nil,}, nil
}

func (up *UDPProxy) Start() error {
	conn, err := net.ListenUDP("udp", up.laddr)
	if err != nil {
		return fmt.Errorf("unable to create UDP Listener: %w", err)
	}
	fmt.Printf("listening on %v\n", up.laddr.String())
	fmt.Printf("creating %d goroutines", runtime.GOMAXPROCS(-1))
	up.conn = conn
	for i := 0; i < runtime.GOMAXPROCS(-1); i++ {
		go up.startPacketProcessor()
	}
	return up.handleUDPPackets()
}

func (up *UDPProxy) handleUDPPackets() error {
	defer func() {
		fmt.Println("closing connection")
		up.conn.Close()
	}()

	for {
		buf := make([]byte, MaxUDPPacketBytes)
		n, _, err := up.conn.ReadFromUDP(buf)
		if err != nil {
			fmt.Printf("unable to read from udp: %v", err)
			continue
		}
		buf = buf[:n]
		go func() { up.in <- buf }()
	}
}

func (up *UDPProxy) startPacketProcessor() {
	defer func() {
		fmt.Println("closing packet processor")
	}()
	for {
		select {
		case buf := <-up.in:
			{
				packet, err := up.parsePacket(buf)
				if err != nil {
					fmt.Println("unable to parse packet: %v",err)
				}
				packet = up.processPacket(packet)
				_, err = up.conn.WriteTo([]byte(packet.Serialize()), up.backend)
				if err != nil {
					fmt.Printf("unable to write full udp message: %v", err)
				}
			}
		}
	}
}

func (up *UDPProxy) parsePacket(buf []byte) (*DogStatsdPacket, error) {
	p := &DogStatsdPacket{}
	err := p.Parse(buf)
	return p, err
}

func (up *UDPProxy) processPacket(packet *DogStatsdPacket) *DogStatsdPacket {
	tags := packet.Tags
	for i, tag := range tags {
		match, err := regexp.Match(up.DogStatsDTagFilterPattern, []byte(tag))
		if err != nil {
			continue
		}
		if match {
			tags[i] = tags[len(tags)-1]
			tags = tags[:len(tags)-1]
		}
	}
	return packet
}
