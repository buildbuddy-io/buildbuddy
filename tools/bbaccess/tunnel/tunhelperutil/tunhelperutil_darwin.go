//go:build darwin && !ios

package tunhelperutil

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// SocketPath is where the helper hands out the device. Only root can create
// files in /var/run.
const SocketPath = "/var/run/bbaccess-tunnel.sock"

const receiveTimeout = 5 * time.Second

// refused is sent instead of the device to a client that may not have it.
const refused = "refused: not the owner"

// Serve listens for connections from bbaccess and hands out the tun fd and
// name if the uid matches (or is root).
// The client connection is kept open so that the client has a signal if the
// helper goes away.
func Serve(l net.Listener, fd int, name string, ownerUID int) {
	for {
		conn, err := l.Accept()
		if errors.Is(err, net.ErrClosed) {
			return
		}
		if err != nil {
			log.Printf("Accepting a connection: %s", err)
			time.Sleep(time.Second)
			continue
		}
		if err := send(conn.(*net.UnixConn), fd, name, ownerUID); err != nil {
			log.Printf("Not handing out %s: %s", name, err)
			conn.Close()
			continue
		}
		go func() {
			io.Copy(io.Discard, conn)
			conn.Close()
		}()
	}
}

func send(conn *net.UnixConn, fd int, name string, ownerUID int) error {
	uid, err := peerUID(conn)
	if err != nil {
		return err
	}
	if uid != ownerUID && uid != 0 {
		conn.Write([]byte(refused))
		return fmt.Errorf("uid %d is not the owner (uid %d)", uid, ownerUID)
	}
	_, _, err = conn.WriteMsgUnix([]byte(name), syscall.UnixRights(fd), nil)
	return err
}

func peerUID(conn *net.UnixConn) (int, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return 0, err
	}
	var uid int
	var cerr error
	if err := raw.Control(func(fd uintptr) { uid, cerr = peerCred(int(fd)) }); err != nil {
		return 0, err
	}
	return uid, cerr
}

// Receive connects to the helper and returns a descriptor for the device, its
// interface name, and the connection, which reads EOF once the helper is gone.
func Receive() (int, string, net.Conn, error) {
	return receive(SocketPath)
}

func receive(path string) (int, string, net.Conn, error) {
	conn, err := net.DialTimeout("unix", path, receiveTimeout)
	if err != nil {
		return 0, "", nil, err
	}
	uc := conn.(*net.UnixConn)
	uc.SetDeadline(time.Now().Add(receiveTimeout))
	buf := make([]byte, 64)
	oob := make([]byte, syscall.CmsgSpace(4))
	n, oobn, _, _, err := uc.ReadMsgUnix(buf, oob)
	if errors.Is(err, io.EOF) {
		err = errors.New("the device helper closed the connection")
	}
	if err == nil && oobn == 0 && n > 0 {
		err = fmt.Errorf("the device helper %s; see its log", buf[:n])
	}
	var fd int
	if err == nil {
		fd, err = oneDescriptor(oob[:oobn])
	}
	if err != nil {
		conn.Close()
		return 0, "", nil, err
	}
	uc.SetDeadline(time.Time{})
	return fd, string(buf[:n]), conn, nil
}

// oneDescriptor returns the descriptor the helper sent, which must be the only
// one.
func oneDescriptor(oob []byte) (int, error) {
	msgs, err := syscall.ParseSocketControlMessage(oob)
	if err != nil {
		return 0, err
	}
	if len(msgs) != 1 {
		return 0, fmt.Errorf("the device helper sent %d control messages, not one", len(msgs))
	}
	fds, err := syscall.ParseUnixRights(&msgs[0])
	if err != nil {
		return 0, err
	}
	if len(fds) != 1 {
		return 0, fmt.Errorf("the device helper sent %d descriptors, not one", len(fds))
	}
	return fds[0], nil
}

func peerCred(fd int) (int, error) {
	cred, err := unix.GetsockoptXucred(fd, unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
	if err != nil {
		return 0, err
	}
	return int(cred.Uid), nil
}
