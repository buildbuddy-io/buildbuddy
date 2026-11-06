// The device helper creates the TUN device the bbaccess tunnel daemon attaches
// to, as root, so that the daemon does not need to be. On Linux it runs once
// per boot from a systemd unit; on macOS launchd keeps it running, because a
// utun lives only while its creator holds it.

// This binary is responsible for setting up the TUN device that the bbaccess
// daemon to handle tunnel traffic.
// The binary ships embedded into bbaccess and bbaccess writes into a system
// local during a one-time install.
// On Linux it's called from a systemd unit and performs a simple tun setup.
// Mac requires the process to stay alive so on mac this starts a daemon that
// hands out the tun fd to bbaccess.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/tunhelperutil"
)

var (
	version = flag.Bool("version", false, "Print the helper's version and exit.")
	uid     = flag.Int("uid", -1, "The user whose tunnel daemon may use the device.")
	cidr    = flag.String("cidr", "", "The address range to route to the device.")
	dev     = flag.String("dev", "", "The device's name, on Linux. macOS assigns one.")
	remove  = flag.Bool("down", false, "Remove the device instead, on Linux.")
)

// mtu is the device's MTU.
const mtu = 1400

func main() {
	flag.Parse()
	if *version {
		fmt.Println(tunhelperutil.Version)
		return
	}
	if !*remove && (*uid < 0 || *cidr == "") {
		fmt.Fprintln(os.Stderr, "usage: tunhelper --uid <uid> --cidr <range> [--dev <name>]\n       tunhelper --down --dev <name>")
		os.Exit(2)
	}
	if os.Geteuid() != 0 {
		log.Fatal("the device helper must run as root")
	}
	var err error
	if *remove {
		err = down(*dev)
	} else {
		err = up(*uid, *cidr, *dev)
	}
	// A failed exit makes launchd relaunch the helper, and systemd report it.
	if err != nil {
		log.Fatal(err)
	}
}
