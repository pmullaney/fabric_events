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

	coreutil "github.com/hyperledger/fabric/core/testutil"
	"github.com/hyperledger/fabric/events/producer"
	"github.com/hyperledger/fabric/msp/mgmt/testtools"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"github.com/hyperledger/fabric/protos/peer"
)

var peerAddress = "0.0.0.0:7303"

func disconnected(err error) {}

var eventsClient *EventsClient

func TestNewEventsClient(t *testing.T) {
	var cases = []struct {
		name     string
		time     int
		expected bool
	}{
		{
			name:     "success",
			time:     5,
			expected: true,
		},
		{
			name:     "fail. regTimout < 100ms",
			time:     0,
			expected: false,
		},
		{
			name:     "fail. regTimeout > 60s",
			time:     61,
			expected: false,
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Logf("Running test: %s", test.name)
			var regTimeout = time.Duration(test.time) * time.Second

			_, err := NewEventsClient(peerAddress, regTimeout, disconnected)
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
			eventsClient, _ = NewEventsClient(test.address, regTimeout, disconnected)
			err = eventsClient.Start()
			if test.expected {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
			eventsClient.Stop()
		})
	}
}

func TestStop(t *testing.T) {
	var err error
	var regTimeout = 5 * time.Second

	eventsClient, _ = NewEventsClient(peerAddress, regTimeout, disconnected)

	if err = eventsClient.Start(); err != nil {
		t.Fail()
		t.Logf("Error client start %s", err)
	}
	err = eventsClient.Stop()
	assert.NoError(t, err)

}

func TestMain(m *testing.M) {
	err := msptesttools.LoadMSPSetupForTesting()
	if err != nil {
		fmt.Printf("Could not initialize msp, err %s", err)
		os.Exit(-1)
		return
	}

	coreutil.SetupTestConfig()
	var opts []grpc.ServerOption
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
