/*
Copyright Hitachi America, Ltd. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consumer

import (
	"fmt"
	"net"
	"os"
	"testing"
	"time"

//	"google.golang.org/grpc/credentials"
//	"google.golang.org/grpc/grpclog"
//	"github.com/hyperledger/fabric/core/config"

	coreutil "github.com/hyperledger/fabric/core/testutil"
	"github.com/hyperledger/fabric/events/producer"
	"github.com/hyperledger/fabric/msp/mgmt/testtools"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/peer"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
//	"github.com/hyperledger/fabric/core/comm"
)

var peerAddress = "0.0.0.0:7303"

var eventsClient *EventsClient

func mockDisconnected(err error) {}

func mockRecvInvalidEventFunc(msg *common.ChannelHeader) {
	return
}

func mockRecvBlockEventFunc(msg *peer.Event_Block) {
	return
}

func mockRecvChaincodeEventFunc(msg *peer.ChaincodeEvent) {
	return
}

func mockRecvTxEventFunc(msg *peer.Transaction) {
	return
}


func TestNewEventsClient(t *testing.T) {
	var cases = []struct {
		name     string
		time     int
		d        disconnectedFunc
		expected bool
	}{
		{
			name:     "success",
			time:     5,
			d:        mockDisconnected,
			expected: true,
		},
		{
			name:     "fail. regTimout < 100ms",
			time:     0,
			d:        mockDisconnected,
			expected: false,
		},
		{
			name:     "fail. regTimeout > 60s",
			time:     61,
			d:        mockDisconnected,
			expected: false,
		},
		{
			name:     "fail nil disconnected func",
			time:     5,
			d:        nil,
			expected: false,
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Logf("Running test: %s", test.name)
			var regTimeout = time.Duration(test.time) * time.Second
			_, err := NewEventsClient(peerAddress, regTimeout, test.d)
			if test.expected {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestNewEventsClientConnectionWithAddress(t *testing.T) {
	var cases = []struct {
		name     string
		address  string
		expected bool
	}{
		{
			name:     "success",
			address:  peerAddress,
			expected: true,
		},
		{
			name:     "fail",
			address:  "",
			expected: false,
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Logf("Running test: %s", test.name)
			_, err := newEventsClientConnectionWithAddress(test.address)
			if test.expected {
				assert.NoError(t, err)
			} else {
			//	fmt.Println("***", err)
				assert.Error(t, err)
			}
		})
	}
}

func TestStart(t *testing.T) {
	var err error
	var regTimeout = 5 * time.Second

	var cases = []struct {
		name     string
		address  string
		expected bool
	}{
		{
			name:     "success",
			address:  peerAddress,
			expected: true,
		},
		{
			name:     "fail no peerAddress",
			address:  "",
			expected: false,
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Logf("Running test: %s", test.name)
			eventsClient, _ = NewEventsClient(test.address, regTimeout, mockDisconnected)
			err = eventsClient.Start()
			if test.expected {
				assert.NoError(t, err)
			} else {
				fmt.Println("***", err)
				assert.Error(t, err)
			}
			eventsClient.Stop()
		})
	}
}

func TestStop(t *testing.T) {
	var err error
	var regTimeout = 5 * time.Second

	eventsClient, _ = NewEventsClient(peerAddress, regTimeout, mockDisconnected)

	if err = eventsClient.Start(); err != nil {
		t.Fail()
		t.Logf("Error client start %s", err)
	}
	err = eventsClient.Stop()
	assert.NoError(t, err)

}

func TestRegisterInvalidEvent(t *testing.T) {
	var err error
	var regTimeout = 5 * time.Second
	var cases = []struct {
		name        string
		ri          recvInvalidEventFunc
		alreadyRegd bool
		expected    bool
	}{
		{
			name:        "success",
			ri:          mockRecvInvalidEventFunc,
			alreadyRegd: false,
			expected:    true,
		},
		{
			name:        "fail no callback function",
			ri:          nil,
			alreadyRegd: false,
			expected:    false,
		},
		{
			name:        "fail already registered",
			ri:          mockRecvInvalidEventFunc,
			alreadyRegd: true,
			expected:    false,
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Logf("Running test: %s", test.name)
			eventsClient, _ = NewEventsClient(peerAddress, regTimeout, mockDisconnected)
			_ = eventsClient.Start()
			if test.alreadyRegd != false {
				if err = eventsClient.RegisterInvalidEvent(test.ri); err != nil {
					t.Fail()
					t.Logf("Error registering first time %s", err)
				}
			}
			err = eventsClient.RegisterInvalidEvent(test.ri)
			if test.expected {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
			eventsClient.Stop()
		})
	}
}

func TestRegisterBlockEvent(t *testing.T) {
	var err error
	var regTimeout = 5 * time.Second
	var cases = []struct {
		name        string
		rb          recvBlockEventFunc
		alreadyRegd bool
		expected    bool
	}{
		{
			name:        "success",
			rb:          mockRecvBlockEventFunc,
			alreadyRegd: false,
			expected:    true,
		},
		{
			name:        "fail no callback function",
			rb:          nil,
			alreadyRegd: false,
			expected:    false,
		},
		{
			name:        "fail already registered",
			rb:          mockRecvBlockEventFunc,
			alreadyRegd: true,
			expected:    false,
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Logf("Running test: %s", test.name)
			eventsClient, _ = NewEventsClient(peerAddress, regTimeout, mockDisconnected)
			_ = eventsClient.Start()
			if test.alreadyRegd != false {
				if err = eventsClient.RegisterBlockEvent(test.rb); err != nil {
					t.Fail()
					t.Logf("Error registering first time %s", err)
				}
			}
			err = eventsClient.RegisterBlockEvent(test.rb)
			if test.expected {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
			eventsClient.Stop()
		})
	}
}

func TestRegisterChaincodeEvent(t *testing.T) {
	var err error
	var regTimeout = 5 * time.Second
	var cases = []struct {
		name                string
		rc                  recvChaincodeEventFunc
		chaincodeEventsList []string
		alreadyRegd         bool
		expected            bool
	}{
		{
			name:                "success",
			rc:                  mockRecvChaincodeEventFunc,
			chaincodeEventsList: []string{"CC1", "EN1"},
			alreadyRegd:         false,
			expected:            true,
		},
		{
			name:                "fail no callback function",
			rc:                  nil,
			chaincodeEventsList: []string{"CC1", "EN1"},
			alreadyRegd:         false,
			expected:            false,
		},
		{
			name:                "fail already registered",
			rc:                  mockRecvChaincodeEventFunc,
			chaincodeEventsList: []string{"CC1", "EN1"},
			alreadyRegd:         true,
			expected:            false,
		},
		{
			name:                "fail not enough chaincode event values passed",
			rc:                  mockRecvChaincodeEventFunc,
			chaincodeEventsList: []string{"EN1"},
			alreadyRegd:         false,
			expected:            false,
		},
		{
			name:                "fail incorrect number of chaincode event values passed",
			rc:                  mockRecvChaincodeEventFunc,
			chaincodeEventsList: []string{"CC1", "EN1", "CC2"},
			alreadyRegd:         false,
			expected:            false,
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Logf("Running test: %s", test.name)
			eventsClient, _ = NewEventsClient(peerAddress, regTimeout, mockDisconnected)
			_ = eventsClient.Start()
			if test.alreadyRegd != false {
				if err = eventsClient.RegisterChaincodeEvents(test.chaincodeEventsList, test.rc); err != nil {
					t.Fail()
					t.Logf("Error registering first time %s", err)
				}
			}
			err = eventsClient.RegisterChaincodeEvents(test.chaincodeEventsList, test.rc)
			if test.expected {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
			eventsClient.Stop()
		})
	}
}

func TestRegisterTxEvents(t *testing.T) {
	var err error
	var regTimeout = 5 * time.Second
	var cases = []struct {
		name        string
		rt          recvTxEventFunc
		txIDsList   []string
		alreadyRegd bool
		expected    bool
	}{
		{
			name:        "success",
			rt:          mockRecvTxEventFunc,
			txIDsList:   []string{"TX1"},
			alreadyRegd: false,
			expected:    true,
		},
		{
			name:        "fail no callback function",
			rt:          nil,
			txIDsList:   []string{"TX1"},
			alreadyRegd: false,
			expected:    false,
		},
		{
			name:        "fail already registered",
			rt:          mockRecvTxEventFunc,
			txIDsList:   []string{"TX1"},
			alreadyRegd: true,
			expected:    false,
		},
		{
			name:        "fail not enough txIDs passed",
			rt:          mockRecvTxEventFunc,
			txIDsList:   []string{},
			alreadyRegd: false,
			expected:    false,
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Logf("Running test: %s", test.name)
			eventsClient, _ = NewEventsClient(peerAddress, regTimeout, mockDisconnected)
			_ = eventsClient.Start()
			if test.alreadyRegd != false {
				if err = eventsClient.RegisterTxEvents(test.txIDsList, test.rt); err != nil {
					t.Fail()
					t.Logf("Error registering first time %s", err)
				}
			}
			err = eventsClient.RegisterTxEvents(test.txIDsList, test.rt)
			if test.expected {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
			eventsClient.Stop()
		})
	}
}

func TestRegisterChannelIDs(t *testing.T) {
	var err error
	var regTimeout = 5 * time.Second
	var cases = []struct {
		name           string
		channelIDsList []string
		alreadyRegd    bool
		expected       bool
	}{
		{
			name:           "success",
			channelIDsList: []string{"C1"},
			alreadyRegd:    false,
			expected:       true,
		},
		{
			name:           "fail already registered",
			channelIDsList: []string{"C1"},
			alreadyRegd:    true,
			expected:       false,
		},
		{
			name:           "fail not enough channelIDs passed",
			channelIDsList: []string{},
			alreadyRegd:    false,
			expected:       false,
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Logf("Running test: %s", test.name)
			eventsClient, _ = NewEventsClient(peerAddress, regTimeout, mockDisconnected)
			_ = eventsClient.Start()
			if test.alreadyRegd != false {
				if err = eventsClient.RegisterChannelIDs(test.channelIDsList); err != nil {
					t.Fail()
					t.Logf("Error registering first time %s", err)
				}
			}
			err = eventsClient.RegisterChannelIDs(test.channelIDsList)
			if test.expected {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
			eventsClient.Stop()
		})
	}
}

func TestUnregisterInvalidEvent(t *testing.T) {
	var err error
	var regTimeout = 5 * time.Second
	var cases = []struct {
		name        string
		ri          recvInvalidEventFunc
		alreadyRegd bool
		expected    bool
	}{
		{
			name:        "success",
			ri:          mockRecvInvalidEventFunc,
			alreadyRegd: true,
			expected:    true,
		},
		{
			name:        "fail not already registered",
			ri:          mockRecvInvalidEventFunc,
			alreadyRegd: false,
			expected:    false,
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Logf("Running test: %s", test.name)
			eventsClient, _ = NewEventsClient(peerAddress, regTimeout, mockDisconnected)
			_ = eventsClient.Start()
			if test.alreadyRegd != false {
				if err = eventsClient.RegisterInvalidEvent(test.ri); err != nil {
					t.Fail()
					t.Logf("Error registering first time %s", err)
				}
			}
			err = eventsClient.UnregisterInvalidEvent()
			if test.expected {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
			eventsClient.Stop()
		})
	}
}

func TestUnregisterBlockEvent(t *testing.T) {
	var err error
	var regTimeout = 5 * time.Second
	var cases = []struct {
		name        string
		rb          recvBlockEventFunc
		alreadyRegd bool
		expected    bool
	}{
		{
			name:        "success",
			rb:          mockRecvBlockEventFunc,
			alreadyRegd: true,
			expected:    true,
		},
		{
			name:        "fail not already registered",
			rb:          mockRecvBlockEventFunc,
			alreadyRegd: false,
			expected:    false,
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Logf("Running test: %s", test.name)
			eventsClient, _ = NewEventsClient(peerAddress, regTimeout, mockDisconnected)
			_ = eventsClient.Start()
			if test.alreadyRegd != false {
				if err = eventsClient.RegisterBlockEvent(test.rb); err != nil {
					t.Fail()
					t.Logf("Error registering first time %s", err)
				}
			}
			err = eventsClient.UnregisterBlockEvent()
			if test.expected {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
			eventsClient.Stop()
		})
	}
}

func TestUnregisterChaincodeEvents(t *testing.T) {
	var err error
	var regTimeout = 5 * time.Second
	var cases = []struct {
		name                     string
		rc                       recvChaincodeEventFunc
		chaincodeEventsListReg   []string
		chaincodeEventsListUnreg []string
		alreadyRegd              bool
		expected                 bool
	}{
		{
			name: "success",
			rc:   mockRecvChaincodeEventFunc,
			chaincodeEventsListReg:   []string{"CC1", "EN1"},
			chaincodeEventsListUnreg: []string{"CC1", "EN1"},
			alreadyRegd:              true,
			expected:                 true,
		},
		{
			name: "fail not already registered",
			rc:   mockRecvChaincodeEventFunc,
			chaincodeEventsListReg:   []string{"CC1", "EN1"},
			chaincodeEventsListUnreg: []string{"CC1", "EN1"},
			alreadyRegd:              false,
			expected:                 false,
		},
		{
			name: "fail not enough chaincode event values passed",
			rc:   mockRecvChaincodeEventFunc,
			chaincodeEventsListReg:   []string{"CC1", "EN1"},
			chaincodeEventsListUnreg: []string{"CC1"},
			alreadyRegd:              true,
			expected:                 false,
		},
		{
			name: "fail incorrect number of chaincode event values passed",
			rc:   mockRecvChaincodeEventFunc,
			chaincodeEventsListReg:   []string{"CC1", "EN1"},
			chaincodeEventsListUnreg: []string{"CC1", "EN1", "CC2"},
			alreadyRegd:              true,
			expected:                 false,
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Logf("Running test: %s", test.name)
			eventsClient, _ = NewEventsClient(peerAddress, regTimeout, mockDisconnected)
			_ = eventsClient.Start()
			if test.alreadyRegd != false {
				if err = eventsClient.RegisterChaincodeEvents(test.chaincodeEventsListReg, test.rc); err != nil {
					t.Fail()
					t.Logf("Error registering first time %s", err)
				}
			}
			err = eventsClient.UnregisterChaincodeEvents(test.chaincodeEventsListUnreg)
			if test.expected {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
			eventsClient.Stop()
		})
	}
}

func TestUnregisterTxEvents(t *testing.T) {
	var err error
	var regTimeout = 5 * time.Second
	var cases = []struct {
		name           string
		rt             recvTxEventFunc
		txIDsListReg   []string
		txIDsListUnreg []string
		alreadyRegd    bool
		expected       bool
	}{
		{
			name:           "success",
			rt:             mockRecvTxEventFunc,
			txIDsListReg:   []string{"TX1"},
			txIDsListUnreg: []string{"TX1"},
			alreadyRegd:    true,
			expected:       true,
		},
		{
			name:           "fail not already registered",
			rt:             mockRecvTxEventFunc,
			txIDsListReg:   []string{"TX1"},
			txIDsListUnreg: []string{"TX1"},
			alreadyRegd:    false,
			expected:       false,
		},
		{
			name:           "fail not enough chaincode event values passed",
			rt:             mockRecvTxEventFunc,
			txIDsListReg:   []string{"TX1"},
			txIDsListUnreg: []string{},
			alreadyRegd:    true,
			expected:       false,
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Logf("Running test: %s", test.name)
			eventsClient, _ = NewEventsClient(peerAddress, regTimeout, mockDisconnected)
			_ = eventsClient.Start()
			if test.alreadyRegd != false {
				if err = eventsClient.RegisterTxEvents(test.txIDsListReg, test.rt); err != nil {
					t.Fail()
					t.Logf("Error registering first time %s", err)
				}
			}
			err = eventsClient.UnregisterTxEvents(test.txIDsListUnreg)
			if test.expected {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
			eventsClient.Stop()
		})
	}
}

func TestUnregisterChannelIDs(t *testing.T) {
	var err error
	var regTimeout = 5 * time.Second
	var cases = []struct {
		name                string
		channelIDsListReg   []string
		channelIDsListUnreg []string
		alreadyRegd         bool
		expected            bool
	}{
		{
			name:                "success",
			channelIDsListReg:   []string{"C1"},
			channelIDsListUnreg: []string{"C1"},
			alreadyRegd:         true,
			expected:            true,
		},
		{
			name:                "fail not already registered",
			channelIDsListReg:   []string{"C1"},
			channelIDsListUnreg: []string{"C1"},
			alreadyRegd:         false,
			expected:            false,
		},
		{
			name:                "fail not enough chaincode event values passed",
			channelIDsListReg:   []string{"C1"},
			channelIDsListUnreg: []string{},
			alreadyRegd:         true,
			expected:            false,
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Logf("Running test: %s", test.name)
			eventsClient, _ = NewEventsClient(peerAddress, regTimeout, mockDisconnected)
			_ = eventsClient.Start()
			if test.alreadyRegd != false {
				if err = eventsClient.RegisterChannelIDs(test.channelIDsListReg); err != nil {
					t.Fail()
					t.Logf("Error registering first time %s", err)
				}
			}
			err = eventsClient.UnregisterChannelIDs(test.channelIDsListUnreg)
			if test.expected {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
			eventsClient.Stop()
		})
	}
}

func TestProcessEvents(t *testing.T) {
	var err error
	var regTimeout = 5 * time.Second

	var cases = []struct {
		name     string
		address  string
		expected bool
	}{
		{
			name:     "success",
			address:  peerAddress,
			expected: true,
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Logf("Running test: %s", test.name)
			eventsClient, _ = NewEventsClient(test.address, regTimeout, mockDisconnected)
			err = eventsClient.Start()
		        creator, err := getCreatorFromLocalMSP()
		        if err != nil {
				t.Fail()
				t.Logf("Error getting creator from MSP %s", err)
		        }
			emsg := &peer.Event{Event: &peer.Event_Block{Block: &common.Block{}}, Creator: creator}
			// SEND BLOCK EVENT
			producer.Send(emsg)
//			time.Sleep(5 * time.Second)

			if test.expected {
				assert.NoError(t, err)
			} else {
				fmt.Println("***", err)
				assert.Error(t, err)
			}
			eventsClient.Stop()
		})
	}
}
/*
func TestProcessEvents(t *testing.T) {
	var err error
	var regTimeout = 5 * time.Second

	var cases = []struct {
		name     string
		address  string
		expected bool
	}{
		{
			name:     "success",
			address:  peerAddress,
			expected: true,
		},
		{
			name:     "fail no peerAddress",
			address:  "",
			expected: false,
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Logf("Running test: %s", test.name)
			eventsClient, _ = NewEventsClient(test.address, regTimeout, mockDisconnected)
			err = eventsClient.Start()

		        creator, err := getCreatorFromLocalMSP()
		        if err != nil {
				t.Fail()
				t.Logf("Error getting creator from MSP %s", err)
		        }
			if eventsClient.stream == nil {
                		t.Fail()
				t.Logf("Error, ec.stream is nil")
        		}
			emsg := &peer.Event{Event: &peer.Event_Block{Block: &common.Block{}}, Creator: creator}
			err = eventsClient.send(emsg)
		        if err != nil {
				t.Fail()
				t.Logf("Error sending message %s", err)
		        }
			if test.expected {
				assert.NoError(t, err)
			} else {
				fmt.Println("***", err)
				assert.Error(t, err)
			}
			eventsClient.Stop()
		})
	}
}
*/
func TestMain(m *testing.M) {
	err := msptesttools.LoadMSPSetupForTesting()
	if err != nil {
		fmt.Printf("Could not initialize msp, err %s", err)
		os.Exit(-1)
		return
	}

	coreutil.SetupTestConfig()

	var opts []grpc.ServerOption

//nc
/*
		creds, err := credentials.NewServerTLSFromFile(config.GetPath("peer.tls.cert.file"), config.GetPath("peer.tls.key.file"))
		if err != nil {
			grpclog.Fatalf("Failed to generate credentials %v", err)
		}
		opts = []grpc.ServerOption{grpc.Creds(creds)}
*/
//endnc

	grpcServer := grpc.NewServer(opts...)

	lis, err := net.Listen("tcp", peerAddress)
	if err != nil {
		fmt.Printf("Error starting events listener %s....not doing tests", err)
		return
	}

	ehServer := producer.NewEventsServer(
		uint(viper.GetInt("peer.events.buffersize")),
		viper.GetDuration("peer.events.timeout"))
	peer.RegisterEventsServer(grpcServer, ehServer)

	go grpcServer.Serve(lis)

	time.Sleep(2 * time.Second)
	os.Exit(m.Run())
}
