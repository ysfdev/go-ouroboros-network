package ouroboros

import (
	"fmt"
	"github.com/cloudstruct/go-ouroboros-network/muxer"
	"github.com/cloudstruct/go-ouroboros-network/protocol/blockfetch"
	"github.com/cloudstruct/go-ouroboros-network/protocol/chainsync"
	"github.com/cloudstruct/go-ouroboros-network/protocol/handshake"
	"io"
	"net"
)

type Ouroboros struct {
	conn               io.ReadWriteCloser
	networkMagic       uint32
	waitForHandshake   bool
	useNodeToNodeProto bool
	handshakeComplete  bool
	muxer              *muxer.Muxer
	ErrorChan          chan error
	// Mini-protocols
	Handshake                *handshake.Handshake
	ChainSync                *chainsync.ChainSync
	chainSyncCallbackConfig  *chainsync.ChainSyncCallbackConfig
	BlockFetch               *blockfetch.BlockFetch
	blockFetchCallbackConfig *blockfetch.BlockFetchCallbackConfig
}

type OuroborosOptions struct {
	Conn         io.ReadWriteCloser
	NetworkMagic uint32
	ErrorChan    chan error
	// Whether to wait for the other side to initiate the handshake. This is useful
	// for servers
	WaitForHandshake         bool
	UseNodeToNodeProtocol    bool
	ChainSyncCallbackConfig  *chainsync.ChainSyncCallbackConfig
	BlockFetchCallbackConfig *blockfetch.BlockFetchCallbackConfig
}

func New(options *OuroborosOptions) (*Ouroboros, error) {
	o := &Ouroboros{
		conn:                     options.Conn,
		networkMagic:             options.NetworkMagic,
		waitForHandshake:         options.WaitForHandshake,
		useNodeToNodeProto:       options.UseNodeToNodeProtocol,
		chainSyncCallbackConfig:  options.ChainSyncCallbackConfig,
		blockFetchCallbackConfig: options.BlockFetchCallbackConfig,
		ErrorChan:                options.ErrorChan,
	}
	if o.ErrorChan == nil {
		o.ErrorChan = make(chan error, 10)
	}
	if o.conn != nil {
		if err := o.setupConnection(); err != nil {
			return nil, err
		}
	}
	return o, nil
}

// Convenience function for creating a connection if you didn't provide one when
// calling New()
func (o *Ouroboros) Dial(proto string, address string) error {
	conn, err := net.Dial(proto, address)
	if err != nil {
		return err
	}
	o.conn = conn
	if err := o.setupConnection(); err != nil {
		return err
	}
	return nil
}

func (o *Ouroboros) setupConnection() error {
	o.muxer = muxer.New(o.conn)
	// Start Goroutine to pass along errors from the muxer
	go func() {
		err := <-o.muxer.ErrorChan
		o.ErrorChan <- err
	}()
	// Perform handshake
	o.Handshake = handshake.New(o.muxer, o.ErrorChan, o.useNodeToNodeProto)
	// TODO: create a proper version map
	versionMap := []uint16{1, 32778}
	if o.useNodeToNodeProto {
		versionMap = []uint16{7}
	}
	if !o.waitForHandshake {
		err := o.Handshake.ProposeVersions(versionMap, o.networkMagic)
		if err != nil {
			return err
		}
	}
	o.handshakeComplete = <-o.Handshake.Finished
	fmt.Printf("negotiated protocol version %d\n", o.Handshake.Version)
	// TODO: register additional mini-protocols
	o.ChainSync = chainsync.New(o.muxer, o.ErrorChan, o.useNodeToNodeProto, o.chainSyncCallbackConfig)
	o.BlockFetch = blockfetch.New(o.muxer, o.ErrorChan, o.blockFetchCallbackConfig)
	return nil
}
