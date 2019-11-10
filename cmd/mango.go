package cmd

import (
	"errors"
	"fmt"
	"github.com/mikeykhalil/mango/pkg/proxy"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"os"
)

const rootDesc = `
Mango is a UDP Proxy that can enforce rules for Dogstatsd messages
`

const (
	AppName = "mango"
	DefaultLogLevel = logrus.InfoLevel
)

var rootCmd = &cobra.Command{
	Use: AppName + " [local_address] [remote_address]",
	Short: fmt.Sprintf("%v is a UDP proxy for Dogstatsd servers", AppName),
	Long: rootDesc,
	PersistentPreRunE: PersistentPreRunE,
	Run: Run,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func PersistentPreRunE(cmd *cobra.Command, args []string) error {
	if len(args) != 2 {
		return errors.New("must provide the address and remote address")
	}
	return nil
}

func Run(cmd *cobra.Command, args []string) {
	laddr, raddr := args[0], args[1]
	p, err := proxy.NewUDPProxy("udp", laddr, raddr)
	if err != nil {
		Exit(err)
	}
	p.Start()
}

func Exit(err error) {
	logrus.Fatalf("error starting mango: %v", err)
}
