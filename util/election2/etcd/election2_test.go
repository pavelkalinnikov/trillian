// Copyright 2018 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package etcd

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/google/trillian/testonly/integration/etcd"
	"github.com/google/trillian/util/election2"
)

func TestElectionThroughCommonClient(t *testing.T) {
	_, client, cleanup, err := etcd.StartEtcd()
	if err != nil {
		t.Fatalf("StartEtcd(): %v", err)
	}
	defer cleanup()

	ctx := context.Background()
	fact := NewFactory("serv", client, "res/")

	el1, err := fact.NewElection(ctx, "10")
	if err != nil {
		t.Fatalf("NewElection(10): %v", err)
	}
	el2, err := fact.NewElection(ctx, "20")
	if err != nil {
		t.Fatalf("NewElection(20): %v", err)
	}

	if err := el1.Await(ctx); err != nil {
		t.Fatalf("Await(10): %v", err)
	}
	if err := el2.Await(ctx); err != nil {
		t.Fatalf("Await(20): %v", err)
	}

	if err := el1.Close(ctx); err != nil {
		t.Fatalf("Close(10): %v", err)
	}
	if err := el2.Close(ctx); err != nil {
		t.Fatalf("Close(20): %v", err)
	}
}

// TODO(pavelkalinnikov): The tests below should be moved to testonly, and
// tested across all implementations.

func TestGetMasterAgreement(t *testing.T) {
	e, client, cleanup, err := etcd.StartEtcd()
	if err != nil {
		t.Fatalf("StartEtcd(): %v", err)
	}
	defer cleanup()

	// Add another active participant.
	client2 := mustCreateClient(t, e)
	defer client2.Close()

	// Add a couple of passive observers.
	ob1 := mustCreateClient(t, e)
	defer ob1.Close()
	ob2 := mustCreateClient(t, e)
	defer ob2.Close()

	ctx := context.Background()
	pfx := "res/"
	facts := []*Factory{
		NewFactory("serv1", client, pfx),
		NewFactory("serv2", client2, pfx),
		NewFactory("ob1", ob1, pfx),
		NewFactory("ob2", ob2, pfx),
	}

	elections := make([]election2.Election, 0)
	for _, f := range facts {
		e, err := f.NewElection(ctx, "10")
		if err != nil {
			t.Fatalf("NewElection(10): %v", err)
		}
		elections = append(elections, e)
	}

	var wg sync.WaitGroup
	for _, e := range elections[0:2] {
		wg.Add(1)
		go func(e election2.Election) {
			defer wg.Done()
			if err := e.Await(ctx); err != nil {
				t.Errorf("Await(10): %v", err)
			}
			id := e.(*Election).instanceID

			checkGetMasterConsistent(ctx, t, id, elections)
			if err := e.Close(ctx); err != nil {
				t.Errorf("Close(10): %v", err)
			}
		}(e)
	}
	wg.Wait()
}

func TestGetMasterReturnsNoLeader(t *testing.T) {
	ctx := context.Background()
	_, client, cleanup, err := etcd.StartEtcd()
	if err != nil {
		t.Fatalf("StartEtcd(): %v", err)
	}
	defer cleanup()
	fact := NewFactory("serv1", client, "res/")
	el, err := fact.NewElection(ctx, "10")
	if err != nil {
		t.Fatalf("NewElection(10): %v", err)
	}
	if err := el.Await(ctx); err != nil {
		t.Errorf("WaitForMastership(10): %v", err)
	}
	if err := el.Close(ctx); err != nil {
		t.Errorf("Close(10): %v", err)
	}
	_, err = el.GetMaster(ctx)
	if want := election2.ErrNoMaster; err != want {
		t.Errorf("GetMaster: err=%v, want %v", err, want)
	}
}

// mustCreateClient creates an etcd client for the passed in server.
func mustCreateClient(t *testing.T, e *embed.Etcd) *clientv3.Client {
	t.Helper()
	ep := e.Config().LCUrls[0].String()
	c, err := clientv3.New(clientv3.Config{Endpoints: []string{ep}})
	if err != nil {
		t.Fatalf("Error creating etcd client: %v", err)
	}
	return c
}

// checkGetMasterConsistent checks that all participants agree on the master.
func checkGetMasterConsistent(ctx context.Context, t *testing.T, want string, els []election2.Election) {
	t.Helper()
	for _, e := range els {
		for {
			got, err := e.GetMaster(ctx)
			if err == election2.ErrNoMaster {
				t.Log("No master...")
				time.Sleep(time.Second)
				continue
			} else if err != nil {
				t.Fatalf("GetMaster: %v", err)
			}
			if got != want {
				t.Errorf("GetMaster: %q, want %q", got, want)
			}
			break
		}
	}
}
