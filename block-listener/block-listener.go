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
	"strings"

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

func createEventClient(eventAddress string, channelIDs, txIDs, chaincodeEvents []string, block bool, invalid bool) *adapter {
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
	if block == false {
		eventsClient.UnregisterBlockEvent()
	}
	if len(channelIDs) != 0 {
		eventsClient.RegisterChannelIDs(channelIDs)
	}
	if len(txIDs) != 0 {
		eventsClient.RegisterTxEvents(txIDs)
	}
	if len(chaincodeEvents) != 0 {
		eventsClient.RegisterChaincodeEvents(chaincodeEvents)
	}
	if invalid == true {
		eventsClient.RegisterInvalidEvent()
	}
	return adapter
}

func main() {
	var eventAddress string
	var channelID string
	var mspDir string
	var mspID string
	var txID string
	var chaincodeEvent string
	var block bool
	var invalid bool
	flag.StringVar(&eventAddress, "events-address", "0.0.0.0:7053", "address of events server")
	flag.StringVar(&channelID, "events-from-channel", "", "listen to events from a given channel - accepts comma separated values: <channelID1,channelID2,...> - default is all")
	flag.StringVar(&mspDir, "events-mspdir", "", "set up the msp direction")
	flag.StringVar(&mspID, "events-mspid", "", "set up the mspid")
	flag.StringVar(&txID, "events-txid", "", "listen to events from a given transaction - accepts comma separated values: <transactionID1,transactionID2,...>")
	flag.StringVar(&chaincodeEvent, "events-chaincode-event", "", "listen to events from a given chaincode with a given event name - accepts comma separated pairs: <chaincodeID1,event-name1,...>")
	flag.BoolVar(&block, "events-block", true, "listen to block events")
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

	var channelIDs []string
	var txIDs []string
	var chaincodeEvents []string
	if len(channelID) != 0 {
		channelIDs = strings.Split(channelID, ",")
	}
	if len(txID) != 0 {
		txIDs = strings.Split(txID, ",")
	}
	if len(chaincodeEvent) != 0 {
		chaincodeEvents = strings.Split(chaincodeEvent, ",")
		if len(chaincodeEvents) % 2 != 0 {
			fmt.Printf("Chaincode events must be entered as comma separated pairs: <chaincodeID1,event-name1,...\n")
			os.Exit(-1)
		}
	}
	fmt.Printf("Event Address: %s\n", eventAddress)
	a := createEventClient(eventAddress, channelIDs, txIDs, chaincodeEvents, block, invalid)
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
