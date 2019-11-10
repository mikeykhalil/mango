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
}

type Config struct {
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
	return &UDPProxy{c, local, remote, make(chan []byte, 100)}, nil
}

func (up *UDPProxy) Start() error {
	conn, err := net.ListenUDP("udp", up.laddr)
	if err != nil {
		return fmt.Errorf("unable to create UDP Listener: %w", err)
	}
	fmt.Println("listening on %v", up.laddr.String())
	for i := 0; i < runtime.GOMAXPROCS(-1); i++ {
		go up.startPacketProcessor()
	}
	return up.handleUDPPackets(conn)
}

func (up *UDPProxy) handleUDPPackets(conn *net.UDPConn) error {
	//defer conn.Close()
	for {
		buf := make([]byte, MaxUDPPacketBytes)
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			fmt.Printf("unable to read from udp: %v", err)
			continue
		}
		buf = buf[:n]
		go func() { up.in <- buf }()
	}
}

func (up *UDPProxy) startPacketProcessor() {
	conn, err := net.Dial("udp", up.backend.String())
	if err != nil {
		return
	}
	for {
		select {
		case buf := <-up.in:
			{
				packet, err := up.parsePacket(buf)
				if err != nil {
					fmt.Println("unable to parse packet: %v",err)
				}
				packet = up.processPacket(packet)
				fmt.Println(string(packet.Serialize()))
				_, err = fmt.Fprintf(conn, packet.Serialize())
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
