package gateway

import (
	"net"

	"github.com/NebulousLabs/muxado" // used pre-1.2
	"github.com/glendc/Sia/build"
	"github.com/xtaci/smux"
)

// returns a new client stream, with a protocol that works on top of the TCP connection.
// using smux for version >= 1.1.2, and using muxado otherwise.
func newClientStream(conn net.Conn, version string) (sess streamSession, err error) {
	if build.IsVersion(version) && build.VersionCmp(version, streamUpgradeVersion) >= 0 {
		sess, err = newSmuxClient(conn)
		return
	}

	sess = newMuxadoClient(conn)
	return
}

// returns a new server stream, with a protocol that works on top of the TCP connection.
// using smux for version >= 1.1.2, and using muxado otherwise.
func newServerStream(conn net.Conn, version string) (sess streamSession, err error) {
	if build.IsVersion(version) && build.VersionCmp(version, streamUpgradeVersion) >= 0 {
		sess, err = newSmuxServer(conn)
		return
	}

	sess = newMuxadoServer(conn)
	return
}

// A streamSession is a multiplexed transport that can accept or initiate
// streams.
type streamSession interface {
	Accept() (net.Conn, error)
	Open() (net.Conn, error)
	Close() error
}

// muxado's Session methods do not return a net.Conn, but rather a
// muxado.Stream, necessitating an adaptor.
type muxadoAdaptor struct {
	sess muxado.Session
}

func (m *muxadoAdaptor) Accept() (net.Conn, error) { return m.sess.Accept() }
func (m *muxadoAdaptor) Open() (net.Conn, error)   { return m.sess.Open() }
func (m *muxadoAdaptor) Close() error              { return m.sess.Close() }

func newMuxadoServer(conn net.Conn) streamSession {
	return &muxadoAdaptor{muxado.Server(conn)}
}

func newMuxadoClient(conn net.Conn) streamSession {
	return &muxadoAdaptor{muxado.Client(conn)}
}

// smux's Session methods do not return a net.Conn, but rather a
// smux.Stream, necessitating an adaptor.
type smuxAdopter struct {
	sess *smux.Session
}

func (s *smuxAdopter) Accept() (net.Conn, error) { return s.sess.AcceptStream() }
func (s *smuxAdopter) Open() (net.Conn, error)   { return s.sess.OpenStream() }
func (s *smuxAdopter) Close() error              { return s.sess.Close() }

func newSmuxServer(conn net.Conn) (streamSession, error) {
	sess, err := smux.Server(conn, nil) // default config
	if err != nil {
		return nil, err
	}
	return &smuxAdopter{sess}, nil
}

func newSmuxClient(conn net.Conn) (streamSession, error) {
	sess, err := smux.Client(conn, nil) // default config
	if err != nil {
		return nil, err
	}
	return &smuxAdopter{sess}, nil
}
