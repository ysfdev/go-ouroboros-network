package handshake

import (
	"fmt"
	"github.com/cloudstruct/go-ouroboros-network/muxer"
	"github.com/cloudstruct/go-ouroboros-network/protocol"
	"github.com/cloudstruct/go-ouroboros-network/utils"
)

const (
	PROTOCOL_NAME = "handshake"
	PROTOCOL_ID   = 0

	STATE_PROPOSE = iota
	STATE_CONFIRM
	STATE_DONE

	DIFFUSION_MODE_INITIATOR_ONLY          = true
	DIFFUSION_MODE_INITIATOR_AND_RESPONDER = false
)

type Handshake struct {
	proto      *protocol.Protocol
	nodeToNode bool
	Version    uint16
	Finished   chan bool
}

func New(m *muxer.Muxer, errorChan chan error, nodeToNode bool) *Handshake {
	h := &Handshake{
		nodeToNode: nodeToNode,
		Finished:   make(chan bool, 1),
	}
	h.proto = protocol.New(PROTOCOL_NAME, PROTOCOL_ID, m, errorChan, h.handleMessage, NewMsgFromCbor)
	h.proto.SetState(STATE_PROPOSE)
	return h
}

func (h *Handshake) handleMessage(msgType uint, msg []byte) error {
	var err error
	switch msgType {
	case MESSAGE_TYPE_PROPOSE_VERSIONS:
		err = h.handleProposeVersions(msg)
	case MESSAGE_TYPE_ACCEPT_VERSION:
		err = h.handleAcceptVersion(msg)
	case MESSAGE_TYPE_REFUSE:
		err = h.handleRefuse(msg)
	default:
		err = fmt.Errorf("%s: received unexpected message type %d", PROTOCOL_NAME, msgType)
	}
	return err
}

func (h *Handshake) ProposeVersions(versions []uint16, networkMagic uint32) error {
	if h.proto.GetState() != STATE_PROPOSE {
		return fmt.Errorf("protocol not in expected state")
	}
	// Create our request
	versionMap := make(map[uint16]interface{})
	for _, version := range versions {
		if h.nodeToNode {
			versionMap[version] = []interface{}{networkMagic, DIFFUSION_MODE_INITIATOR_ONLY}
		} else {
			versionMap[version] = networkMagic
		}
	}
	msg := newMsgProposeVersions(versionMap)
	// Send request
	h.proto.SendMessage(msg, false)
	// Set the new state
	h.proto.SetState(STATE_CONFIRM)
	return nil
}

func (h *Handshake) handleProposeVersions(msg []byte) error {
	if h.proto.GetState() != STATE_CONFIRM {
		return fmt.Errorf("received handshake request when protocol is in wrong state")
	}
	// TODO: implement me
	return fmt.Errorf("handshake request handling not yet implemented")
}

func (h *Handshake) handleAcceptVersion(msg []byte) error {
	if h.proto.GetState() != STATE_CONFIRM {
		return fmt.Errorf("received handshake accept response when protocol is in wrong state")
	}
	var resp msgAcceptVersion
	if _, err := utils.CborDecode(msg, &resp); err != nil {
		return fmt.Errorf("%s: decode error: %s", PROTOCOL_NAME, err)
	}
	h.Version = resp.Version
	h.Finished <- true
	h.proto.SetState(STATE_DONE)
	return nil
}

func (h *Handshake) handleRefuse(msg []byte) error {
	if h.proto.GetState() != STATE_CONFIRM {
		return fmt.Errorf("received handshake refuse response when protocol is in wrong state")
	}
	var resp msgRefuse
	if _, err := utils.CborDecode(msg, &resp); err != nil {
		return fmt.Errorf("%s: decode error: %s", PROTOCOL_NAME, err)
	}
	var err error
	switch resp.Reason[0].(uint64) {
	case REFUSE_REASON_VERSION_MISMATCH:
		err = fmt.Errorf("handshake failed: version mismatch")
	case REFUSE_REASON_DECODE_ERROR:
		err = fmt.Errorf("handshake failed: decode error: %s", resp.Reason[2].(string))
	case REFUSE_REASON_REFUSED:
		err = fmt.Errorf("handshake failed: refused: %s", resp.Reason[2].(string))
	}
	h.proto.SetState(STATE_DONE)
	return err
}
