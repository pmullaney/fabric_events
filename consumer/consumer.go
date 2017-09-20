/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package consumer

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/comm"
	"github.com/hyperledger/fabric/core/ledger/util"
	mspmgmt "github.com/hyperledger/fabric/msp/mgmt"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/peer"
	"github.com/hyperledger/fabric/protos/utils"
)

var consumerLogger = flogging.MustGetLogger("eventhub_consumer")

type recvBlockEventFunc func(*peer.Event_Block)

type recvChaincodeEventFunc func(*peer.ChaincodeEvent)

type recvTxEventFunc func(*peer.Transaction)

type recvInvalidEventFunc func(*common.ChannelHeader)

type disconnectedFunc func(error)

type eventHolder struct {
	chaincodeID string
	eventName   string
}

//EventsClient holds the stream and adapter for consumer to work with
type EventsClient struct {
	sync.RWMutex
	peerAddress     string
	regTimeout      time.Duration
	stream          peer.Events_ChatClient
	disconnected    disconnectedFunc
	regBlock        map[bool]recvBlockEventFunc
	channelIDs      map[string]int
	chaincodeEvents map[eventHolder]recvChaincodeEventFunc
	txIDs           map[string]recvTxEventFunc
	regInvalid      map[bool]recvInvalidEventFunc
}

//NewEventsClient Returns a new grpc.ClientConn to the configured local PEER.
func NewEventsClient(peerAddress string, regTimeout time.Duration, disconnected disconnectedFunc) (*EventsClient, error) {
	var err error
	if disconnected == nil {
		err = fmt.Errorf("disconnected function must be provided")
	}
	var emptyRegBlockMap = make(map[bool]recvBlockEventFunc)
	var emptyChannelIDsMap = make(map[string]int)
	var emptyChaincodeEventsMap = make(map[eventHolder]recvChaincodeEventFunc)
	var emptyTxIDsMap = make(map[string]recvTxEventFunc)
	var emptyRegInvalidMap = make(map[bool]recvInvalidEventFunc)
	if regTimeout < 100*time.Millisecond {
		regTimeout = 100 * time.Millisecond
		err = fmt.Errorf("regTimeout >= 0, setting to 100 msec")
	} else if regTimeout > 60*time.Second {
		regTimeout = 60 * time.Second
		err = fmt.Errorf("regTimeout > 60, setting to 60 sec")
	}
	if len(peerAddress) == 0 {
		err = fmt.Errorf("peer address must be provided")
	}
	return &EventsClient{sync.RWMutex{}, peerAddress, regTimeout, nil, disconnected, emptyRegBlockMap, emptyChannelIDsMap, emptyChaincodeEventsMap, emptyTxIDsMap, emptyRegInvalidMap}, err
}

//newEventsClientConnectionWithAddress Returns a new grpc.ClientConn to the configured local PEER.
func newEventsClientConnectionWithAddress(peerAddress string) (*grpc.ClientConn, error) {
	if comm.TLSEnabled() {
		return comm.NewClientConnectionWithAddress(peerAddress, true, true, comm.InitTLSForPeer())
	}
	return comm.NewClientConnectionWithAddress(peerAddress, true, false, nil)
}

func (ec *EventsClient) send(emsg *peer.Event) error {
	ec.Lock()
	defer ec.Unlock()

	// obtain the default signing identity for this peer; it will be used to sign the event
	localMsp := mspmgmt.GetLocalMSP()
	if localMsp == nil {
		return errors.New("nil local MSP manager")
	}

	signer, err := localMsp.GetDefaultSigningIdentity()
	if err != nil {
		return fmt.Errorf("could not obtain the default signing identity, err %s", err)
	}

	//pass the signer's cert to Creator
	signerCert, err := signer.Serialize()
	if err != nil {
		return fmt.Errorf("fail to serialize the default signing identity, err %s", err)
	}
	emsg.Creator = signerCert

	signedEvt, err := utils.GetSignedEvent(emsg, signer)
	if err != nil {
		return fmt.Errorf("could not sign outgoing event, err %s", err)
	}

	return ec.stream.Send(signedEvt)
}

// register - registers interest in a event
func (ec *EventsClient) register(ies []*peer.Interest) error {
	creator, err := getCreatorFromLocalMSP()
	if err != nil {
		return fmt.Errorf("error getting creator from MSP: %s", err)
	}
	emsg := &peer.Event{Event: &peer.Event_Register{Register: &peer.Register{Events: ies}}, Creator: creator}

	if err = ec.send(emsg); err != nil {
		consumerLogger.Errorf("error on Register send %s\n", err)
	}

	regChan := make(chan struct{})
	go func() {
		defer close(regChan)
		in, inerr := ec.stream.Recv()
		if inerr != nil {
			err = inerr
			return
		}
		switch in.Event.(type) {
		case *peer.Event_Register:
		case nil:
			err = fmt.Errorf("invalid nil object for register")
		default:
			err = fmt.Errorf("invalid registration object")
		}
	}()
	select {
	case <-regChan:
	case <-time.After(ec.regTimeout):
		err = fmt.Errorf("timeout waiting for registration")
	}
	return err
}

// RegisterInvalidEvent - registers interest in invalid events
func (ec *EventsClient) RegisterInvalidEvent(ri recvInvalidEventFunc) error {
	if _, exists := ec.regInvalid[true]; exists {
		return fmt.Errorf("error registering for invalid events, already registered")
	}
	if ri == nil {
		return fmt.Errorf("error registering for invalid events, nil callback function passed as argument")
	}
	ec.regInvalid[true] = ri
	return nil
}

// UnregisterInvalidEvent - unregisters interest in invalid events
func (ec *EventsClient) UnregisterInvalidEvent() error {
	if _, exists := ec.regInvalid[true]; !exists {
		return fmt.Errorf("error unregistering for invalid events, not registered")
	}
	delete(ec.regInvalid, true)
	return nil
}

// RegisterBlockEvent - registers interest in block events
func (ec *EventsClient) RegisterBlockEvent(rb recvBlockEventFunc) error {
	if _, exists := ec.regBlock[true]; exists {
		return fmt.Errorf("error registering for block events, already registered")
	}
	if rb == nil {
		return fmt.Errorf("error registering for block events, nil callback function passed as argument")
	}
	ec.regBlock[true] = rb
	return nil
}

// UnregisterBlockEvent - unregisters interest in block events
func (ec *EventsClient) UnregisterBlockEvent() error {
	if _, exists := ec.regBlock[true]; !exists {
		return fmt.Errorf("error unregistering for block events, not registered")
	}
	delete(ec.regBlock, true)
	return nil
}

// RegisterChaincodeEvents - registers interest in chaincode event(s)
func (ec *EventsClient) RegisterChaincodeEvents(chaincodeEventsList []string, rc recvChaincodeEventFunc) error {
	if len(chaincodeEventsList) < 2 {
		return fmt.Errorf("error registering for chaincode event, at least one chaincode ID and one event name must be provided")
	}
	if len(chaincodeEventsList)%2 != 0 {
		return fmt.Errorf("error registering for chaincode event, chaincode events must be passed in pairs with at least one chaincode ID and one event name")
	}
	if rc == nil {
		return fmt.Errorf("error registering for chaincode event, nil callback function passed as argument")
	}
	for i := range chaincodeEventsList {
		if i%2 == 0 {
			event := eventHolder{chaincodeID: chaincodeEventsList[i], eventName: chaincodeEventsList[i+1]}
			if _, exists := ec.chaincodeEvents[event]; exists {
				return fmt.Errorf("error registering for chaincode event, already subscribed to event: %v, on chaincode ID: %v", chaincodeEventsList[i+1], chaincodeEventsList[i])
			}
			ec.chaincodeEvents[event] = rc
		}
	}
	return nil
}

// UnregisterChaincodeEvents - unregisters interest in chaincode event(s)
func (ec *EventsClient) UnregisterChaincodeEvents(chaincodeEventsList []string) error {
	if len(chaincodeEventsList) < 2 {
		return fmt.Errorf("error registering for chaincode event, at least one chaincode ID and one event name must be provided")
	}
	if len(chaincodeEventsList)%2 != 0 {
		return fmt.Errorf("error registering for chaincode event, chaincode events must be passed in pairs with at least one chaincode ID and one event name")
	}
	for i := range chaincodeEventsList {
		if i%2 == 0 {
			event := eventHolder{chaincodeID: chaincodeEventsList[i], eventName: chaincodeEventsList[i+1]}
			if _, exists := ec.chaincodeEvents[event]; exists {
				delete(ec.chaincodeEvents, event)
			} else {
				return fmt.Errorf("error unregistering chaincode event: %v, on chaincode ID: %v, not registered", chaincodeEventsList[i+1], chaincodeEventsList[i])
			}
		}
	}
	return nil
}

// RegisterTxEvents - registers interest in tx event(s)
func (ec *EventsClient) RegisterTxEvents(txIDsList []string, rt recvTxEventFunc) error {
	if len(txIDsList) == 0 {
		return fmt.Errorf("error registering for tx event(s), at least one txID must be provided")
	}
	if rt == nil {
		return fmt.Errorf("error registering for tx event, nil callback function passed as argument")
	}
	for _, input := range txIDsList {
		if _, exists := ec.txIDs[input]; exists {
			return fmt.Errorf("error registering for tx event: %v, already registered", input)
		}
		ec.txIDs[input] = rt
	}
	return nil
}

// UnregisterTxEvents - unregisters interest in tx event(s)
func (ec *EventsClient) UnregisterTxEvents(txIDsList []string) error {
	if len(txIDsList) == 0 {
		return fmt.Errorf("error unregistering for tx event(s), at lease one txID must be provided")
	}
	for _, input := range txIDsList {
		if _, exists := ec.txIDs[input]; exists {
			delete(ec.txIDs, input)
		} else {
			return fmt.Errorf("error unregistering tx event: %v, not registered", input)
		}
	}
	return nil
}

// RegisterChannelIDs - registers interest in events on specific channelID(s)
func (ec *EventsClient) RegisterChannelIDs(channelIDsList []string) error {
	if len(channelIDsList) == 0 {
		return fmt.Errorf("error registering for channel ID event(s), at least one channelID must be provided")
	}
	for _, input := range channelIDsList {
		if _, exists := ec.channelIDs[input]; exists {
			return fmt.Errorf("error registering for channel ID event: %v, already registered", input)
		}
		ec.channelIDs[input] = 0
	}
	return nil
}

// UnregisterChannelIDs - unregisters interest in events on specific channelID(s)
func (ec *EventsClient) UnregisterChannelIDs(channelIDsList []string) error {
	if len(channelIDsList) == 0 {
		return fmt.Errorf("error unregistering for channel ID event(s), at least one channelID must be provided")
	}
	for _, input := range channelIDsList {
		if _, exists := ec.channelIDs[input]; exists {
			delete(ec.channelIDs, input)
		} else {
			return fmt.Errorf("error unregistering channel ID event: %v, not registered")
		}
	}
	return nil
}

// Recv receives next event - use when client has not called Start
func (ec *EventsClient) Recv() (*peer.Event, error) {
	in, err := ec.stream.Recv()
	if err == io.EOF {
		// read done.
		ec.disconnected(nil)
		return nil, err
	}
	if err != nil {
		ec.disconnected(err)
		return nil, err
	}
	return in, nil
}

func (ec *EventsClient) processEvents() error {
	defer ec.stream.CloseSend()
	for {
		in, err := ec.stream.Recv()
		if err == io.EOF {
			// read done.
			ec.disconnected(nil)
			return nil
		}
		if err != nil {
			ec.disconnected(err)
			return err
		}
		if _, ok := in.Event.(*peer.Event_Block); !ok {
			fmt.Println("warning, non Event_Block sent to processEvents, ignoring event")
			continue
		}
		block := in.Event.(*peer.Event_Block).Block
		txsFltr := util.TxValidationFlags(block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER])
		for i, ebytes := range block.Data.Data {
			if ebytes != nil {
				if env, err := utils.GetEnvelopeFromBlock(ebytes); err != nil {
					return fmt.Errorf("error getting tx from block(%s)", err)
				} else if env != nil {
					// get the payload from the envelope
					payload, err := utils.GetPayload(env)
					if err != nil {
						return fmt.Errorf("could not extract payload from envelope, err %s", err)
					}
					chdr, err := utils.UnmarshalChannelHeader(payload.Header.ChannelHeader)
					if err != nil {
						return err
					}
					// Channel ID logic
					// set registered for all channels
					regChannelID := true
					// check if registered for specific channel(s)
					if len(ec.channelIDs) != 0 {
						regChannelID = false
						if _, exists := ec.channelIDs[chdr.ChannelId]; exists {
							regChannelID = true
						}
					}
					if regChannelID == false {
						continue
					}
					// Block event logic
					if regBlockFunc, exists := ec.regBlock[true]; exists {
						// Used to send block event
						regBlockFunc(in.Event.(*peer.Event_Block))
					}
					// Invalid event logic
					if regInvalidFunc, exists := ec.regInvalid[true]; exists {
						if txsFltr.IsInvalid(i) {
							regInvalidFunc(chdr)
						}
					}
					if len(ec.txIDs) != 0 || len(ec.chaincodeEvents) != 0 {
						if common.HeaderType(chdr.Type) == common.HeaderType_ENDORSER_TRANSACTION {
							tx, err := utils.GetTransaction(payload.Data)
							if err != nil {
								return fmt.Errorf("error unmarshalling transaction payload for block event: %s", err)
							}
							// Tx event logic
							if len(ec.txIDs) != 0 {
								if txIDFunc, exists := ec.txIDs[chdr.TxId]; exists {
									// Used to send txEvent
									txIDFunc(tx)
								}
							}
							// Chaincode event logic
							if len(ec.chaincodeEvents) != 0 {
								chaincodeActionPayload, err := utils.GetChaincodeActionPayload(tx.Actions[0].Payload)
								if err != nil {
									return fmt.Errorf("error unmarshalling transaction action payload for block event: %s", err)
								}
								propRespPayload, err := utils.GetProposalResponsePayload(chaincodeActionPayload.Action.ProposalResponsePayload)
								if err != nil {
									return fmt.Errorf("error unmarshalling proposal response payload for block event: %s", err)
								}
								caPayload, err := utils.GetChaincodeAction(propRespPayload.Extension)
								if err != nil {
									return fmt.Errorf("Error unmarshalling chaincode action for block event: %s", err)
								}
								ccEvent, err := utils.GetChaincodeEvents(caPayload.Events)
								if ccEvent != nil {
									event := eventHolder{chaincodeID: ccEvent.ChaincodeId, eventName: ccEvent.EventName}
									if recvChaincodeEventFunc, exists := ec.chaincodeEvents[event]; exists {
										// Used to send ccEvent
										recvChaincodeEventFunc(ccEvent)
									}
								}
							}
						}
					}
				}
			}
		}
	}
}

//Start establishes connection with Event hub and registers interested events with it
func (ec *EventsClient) Start() error {
	conn, err := newEventsClientConnectionWithAddress(ec.peerAddress)
	if err != nil {
		return fmt.Errorf("could not create client conn to %s:%s", ec.peerAddress, err)
	}

	serverClient := peer.NewEventsClient(conn)
	ec.stream, err = serverClient.Chat(context.Background())
	if err != nil {
		return fmt.Errorf("could not create client conn to %s:%s", ec.peerAddress, err)
	}

	ies := []*peer.Interest{{EventType: peer.EventType_BLOCK}}

	if err := ec.register(ies); err != nil {
		return err
	}

	go ec.processEvents()

	return nil
}

//Stop terminates connection with event hub and unregisters for block events from the fabric
func (ec *EventsClient) Stop() error {
	if ec.stream == nil {
		// in case the steam/chat server has not been established earlier, we assume that it's closed, successfully
		return nil
	}

	ies := []*peer.Interest{{EventType: peer.EventType_BLOCK}}

	creator, err := getCreatorFromLocalMSP()
	if err != nil {
		return fmt.Errorf("error getting creator from MSP: %s", err)
	}
	emsg := &peer.Event{Event: &peer.Event_Unregister{Unregister: &peer.Unregister{Events: ies}}, Creator: creator}

	if err := ec.send(emsg); err != nil {
		return fmt.Errorf("error on unregister send %s", err)
	}

	return ec.stream.CloseSend()
}

func getCreatorFromLocalMSP() ([]byte, error) {
	localMsp := mspmgmt.GetLocalMSP()
	if localMsp == nil {
		return nil, errors.New("nil local MSP manager")
	}
	signer, err := localMsp.GetDefaultSigningIdentity()
	if err != nil {
		return nil, fmt.Errorf("could not obtain the default signing identity, err %s", err)
	}
	creator, err := signer.Serialize()
	if err != nil {
		return nil, fmt.Errorf("error serializing the signer: %s", err)
	}
	return creator, nil
}
