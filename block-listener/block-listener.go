/*
 Copyright IBM Corp All Rights Reserved.

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

package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/hyperledger/fabric/events/consumer"
	"github.com/hyperledger/fabric/msp/mgmt"
	"github.com/hyperledger/fabric/msp/mgmt/testtools"
	"github.com/hyperledger/fabric/protos/common"
	pb "github.com/hyperledger/fabric/protos/peer"
)

type adapter struct {
	notifyBlock     chan *pb.Event_Block
	notifyChaincode chan *pb.ChaincodeEvent
	notifyTx        chan *pb.Transaction
	notifyInvalid   chan *common.ChannelHeader
}

//GetInterestedEvents implements consumer.EventAdapter interface for registering interested events
/*func (a *adapter) GetInterestedEvents() ([]*pb.Interest, error) {
	return []*pb.Interest{{EventType: pb.EventType_BLOCK}}, nil
}*/

//Recv implements consumer.EventAdapter interface for receiving events
func (a *adapter) Recv(msg *pb.Event) (bool, error) {
	if o, e := msg.Event.(*pb.Event_Block); e {
		a.notifyBlock <- o
		return true, nil
	}
	return false, fmt.Errorf("Receive unknown type event: %v", msg)
}

func (a *adapter) RecvChaincodeEvent(msg *pb.ChaincodeEvent) bool {
	a.notifyChaincode <- msg
	return true
}

func (a *adapter) RecvTxEvent(msg *pb.Transaction) bool {
	a.notifyTx <- msg
	return true
}

func (a *adapter) RecvInvalidEvent(msg *common.ChannelHeader) bool {
	a.notifyInvalid <- msg
	return true
}

//Disconnected implements consumer.EventAdapter interface for disconnecting
func (a *adapter) Disconnected(err error) {
	fmt.Print("Disconnected...exiting\n")
	os.Exit(1)
}

func createEventClient(eventAddress string, regBlock bool, chaincodeID, eventName, txID string, regInvalid bool) *adapter {
	var eventsClient *consumer.EventsClient

	done := make(chan *pb.Event_Block)
	doneChaincode := make(chan *pb.ChaincodeEvent)
	doneTx := make(chan *pb.Transaction)
	doneInvalid := make(chan *common.ChannelHeader)
	adapter := &adapter{notifyBlock: done, notifyChaincode: doneChaincode, notifyTx: doneTx, notifyInvalid: doneInvalid}
	eventsClient, err := consumer.NewEventsClient(eventAddress, 5, adapter)
	if err != nil {
		fmt.Println(err)
	}
	if err := eventsClient.Start(); err != nil {
		fmt.Printf("could not start chat. err: %s\n", err)
		eventsClient.Stop()
		return nil
	}
	if regBlock == true {
		eventsClient.RegisterBlockEvent()
	}
	if chaincodeID != "" && eventName == "" || chaincodeID == "" && eventName != "" {
		fmt.Println("both chaincodeID and event name must be provided if event name is provided")
		return nil
	}
	if chaincodeID != "" && eventName != "" {
		eventsClient.RegisterChaincodeEvent(chaincodeID, eventName)
	}
	if txID != "" {
		eventsClient.RegisterTxEvent(txID)
	}
	if regInvalid == true {
		eventsClient.RegisterInvalidEvent()
	}
	return adapter
}

func main() {
	var eventAddress string
	var chaincodeID string
	var mspDir string
	var mspID string
	var txID string
	var eventName string
	var block bool
	var invalid bool
	flag.StringVar(&eventAddress, "events-address", "0.0.0.0:7053", "address of events server")
	flag.StringVar(&chaincodeID, "events-from-chaincode", "", "listen to events from given chaincode")
	flag.StringVar(&mspDir, "events-mspdir", "", "set up the msp direction")
	flag.StringVar(&mspID, "events-mspid", "", "set up the mspid")
	flag.StringVar(&txID, "events-txid", "", "listen to events from a given transaction")
	flag.StringVar(&eventName, "events-event-name", "", "listen to events with a given name")
	flag.BoolVar(&block, "events-block", false, "listen to block events")
	flag.BoolVar(&invalid, "events-invalid", false, "listen to invalid events")
	flag.Parse()

	//if no msp info provided, we use the default MSP under fabric/sampleconfig
	if mspDir == "" {
		err := msptesttools.LoadMSPSetupForTesting()
		if err != nil {
			fmt.Printf("Could not initialize msp, err: %s\n", err)
			os.Exit(-1)
		}
	} else {
		//load msp info
		err := mgmt.LoadLocalMsp(mspDir, nil, mspID)
		if err != nil {
			fmt.Printf("Could not initialize msp, err: %s\n", err)
			os.Exit(-1)
		}
	}

	fmt.Printf("Event Address: %s\n", eventAddress)

	a := createEventClient(eventAddress, block, chaincodeID, eventName, txID, invalid)
	if a == nil {
		fmt.Println("Error creating event client")
		return
	}
	for {
		select {
		case bl := <-a.notifyBlock:
			fmt.Println("")
			fmt.Println("")
			fmt.Println("Received block")
			fmt.Println("--------------")
			fmt.Println(bl)
		case cc := <-a.notifyChaincode:
			fmt.Println("")
			fmt.Println("")
			fmt.Println("Received chaincode event")
			fmt.Println("--------------")
			fmt.Println(cc)
		case tx := <-a.notifyTx:
			fmt.Println("")
			fmt.Println("")
			fmt.Println("Received tx event")
			fmt.Println("--------------")
			fmt.Println(tx)
		case in := <-a.notifyInvalid:
			fmt.Println("")
			fmt.Println("")
			fmt.Printf("Received invalid transaction from channel '%s'\n", in.ChannelId)
			fmt.Println("--------------")
			fmt.Printf("Transaction invalid: TxID: %s\n", in.TxId)
		}
	}
}
