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
	"sync"

	"github.com/hyperledger/fabric/events/consumer"
	"github.com/hyperledger/fabric/msp/mgmt"
	"github.com/hyperledger/fabric/msp/mgmt/testtools"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/peer"
)

var wg sync.WaitGroup

//Disconnected implements consumer.EventAdapter interface for disconnecting
func disconnected(err error) {
	if err != nil {
		fmt.Printf("Error on disconnect: %v", err)
	}
	fmt.Printf("Disconnected...exiting\n")
	wg.Done()
	os.Exit(1)
}

func createEventClient(eventAddress string, channelIDs, txIDs, chaincodeEvents []string, block bool, invalid bool) error {

	var eventsClient *consumer.EventsClient

	eventsClient, err := consumer.NewEventsClient(eventAddress, 5, disconnected)
	if err != nil {
		fmt.Println(err)
	}
	if err := eventsClient.Start(); err != nil {
		fmt.Printf("could not start chat. err: %s\n", err)
		eventsClient.Stop()
	}
	if block == true {
		err := eventsClient.RegisterBlockEvent(func(msg *peer.Event_Block) {
			fmt.Println("")
			fmt.Println("")
			fmt.Println("Received block")
			fmt.Println("--------------")
			fmt.Println(msg)
		})
		if err != nil {
			return err
		}
	}
	if len(channelIDs) != 0 {
		err := eventsClient.RegisterChannelIDs(channelIDs)
		if err != nil {
			return err
		}
	}
	if len(txIDs) != 0 {
		err := eventsClient.RegisterTxEvents(txIDs, func(msg *peer.Transaction) {
			fmt.Println("")
			fmt.Println("")
			fmt.Println("Received tx event")
			fmt.Println("--------------")
			fmt.Println(msg)
		})
		if err != nil {
			return err
		}
	}
	if len(chaincodeEvents) != 0 {
		err := eventsClient.RegisterChaincodeEvents(chaincodeEvents, func(msg *peer.ChaincodeEvent) {
			fmt.Println("")
			fmt.Println("")
			fmt.Println("Received chaincode event")
			fmt.Println("--------------")
			fmt.Println(msg)
		})
		if err != nil {
			return err
		}
	}
	if invalid == true {
		err := eventsClient.RegisterInvalidEvent(func(msg *common.ChannelHeader) {
			fmt.Println("")
			fmt.Println("")
			fmt.Printf("Received invalid transaction from channel '%s'\n", msg.ChannelId)
			fmt.Println("--------------")
			fmt.Printf("Transaction invalid: TxID: %s\n", msg.TxId)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func main() {
	wg.Add(1)
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
		if len(chaincodeEvents)%2 != 0 {
			fmt.Printf("Chaincode events must be entered as comma separated pairs: <chaincodeID1,event-name1,...\n")
			os.Exit(-1)
		}
	}

	fmt.Printf("Event Address: %s\n", eventAddress)
	err := createEventClient(eventAddress, channelIDs, txIDs, chaincodeEvents, block, invalid)
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	wg.Wait()
}
