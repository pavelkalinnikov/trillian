// Copyright 2017 Google Inc. All Rights Reserved.
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

// Package etcd holds an etcd-specific implementation of the
// util.MasterElection interface.
package etcd

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/golang/glog"
	"github.com/google/trillian/util/election"
)

var (
	AlreadyStartedErr = errors.New("already started")
	NotStartedErr     = errors.New("not started")
)

// MasterElection is an implementation of util.MasterElection based on etcd.
type MasterElection struct {
	instanceID string
	treeID     int64
	lockFile   string
	client     *clientv3.Client
	election   *concurrency.Election

	masterID string
	masterMu sync.RWMutex
	cancel   context.CancelFunc
}

func (me *MasterElection) updateMaster(id string) {
	me.masterMu.Lock()
	defer me.masterMu.Unlock()
	me.masterID = id
}

func (me *MasterElection) Start(ctx context.Context) error {
	if me.cancel != nil {
		return AlreadyStartedErr
	}
	cctx, cancel := context.WithCancel(ctx)
	me.cancel = cancel

	upds := me.election.Observe(cctx)
	go func() {
		for upd := range upds {
			me.updateMaster(string(upd.Kvs[0].Value))
		}
		me.updateMaster("")
	}()
	return nil
}

// WaitForMastership blocks until the current instance is master.
func (me *MasterElection) WaitForMastership(ctx context.Context) error {
	me.masterMu.Lock()
	defer me.masterMu.Unlock()
	if err := me.election.Campaign(ctx, me.instanceID); err != nil {
		return err
	}
	me.masterID = me.instanceID
	return nil
}

// IsMaster returns whether the current instance is the master.
func (me *MasterElection) IsMaster(ctx context.Context) (bool, error) {
	me.masterMu.RLock()
	defer me.masterMu.RUnlock()
	return me.masterID == me.instanceID, nil
}

// Resign releases mastership.
func (me *MasterElection) Resign(ctx context.Context) error {
	return me.election.Resign(ctx)
}

// Resign releases mastership.
func (me *MasterElection) Close(ctx context.Context) error {
	if me.cancel != nil {
		me.cancel()
	}
	return nil
}

// GetCurrentMaster returns the instanceID of the current master, if any.
func (me *MasterElection) GetCurrentMaster(ctx context.Context) (string, error) {
	me.masterMu.RLock()
	defer me.masterMu.RUnlock()
	return me.masterID, nil
}

// ElectionFactory creates etcd.MasterElection instances.
type ElectionFactory struct {
	client     *clientv3.Client
	session    *concurrency.Session
	instanceID string
	lockDir    string
}

// NewElectionFactory builds an election factory that uses the given
// parameters. The passed in etcd client should remain valid for the lifetime
// of the ElectionFactory.
func NewElectionFactory(instanceID string, client *clientv3.Client, lockDir string) *ElectionFactory {
	return &ElectionFactory{
		client:     client,
		instanceID: instanceID,
		lockDir:    lockDir,
	}
}

func (ef ElectionFactory) Start(ctx context.Context) error {
	if ef.session != nil {
		return AlreadyStartedErr
	}
	session, err := concurrency.NewSession(ef.client)
	if err != nil {
		return err
	}
	ef.session = session
	return nil
}

// NewElection creates a specific etcd.MasterElection instance.
func (ef ElectionFactory) NewElection(ctx context.Context, treeID int64) (election.MasterElection, error) {
	if ef.session == nil {
		return nil, NotStartedErr
	}

	lockFile := fmt.Sprintf("%s/%d", strings.TrimRight(ef.lockDir, "/"), treeID)
	election := concurrency.NewElection(ef.session, lockFile)

	me := MasterElection{
		instanceID: ef.instanceID,
		treeID:     treeID,
		lockFile:   lockFile,
		client:     ef.client,
		election:   election,
	}
	glog.Infof("MasterElection created: %+v", me)
	return &me, nil
}
