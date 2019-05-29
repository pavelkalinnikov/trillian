// Copyright 2019 Google Inc. All Rights Reserved.
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

package log

import (
	"context"
	"fmt"
	"sort"

	"github.com/google/trillian/extension"
)

// Tracker tracks IDs of active Trillian logs.
type Tracker struct {
	reg extension.Registry
	ids []int64 // Sorted in ascending order.
}

// Update updates the list of active log IDs from the log storage. Returns the
// list added IDs and the list of deleted IDs.
func (t *Tracker) Update(ctx context.Context) ([]int64, []int64, error) {
	old := t.ids
	ids, err := t.getActiveLogIDs(ctx)
	if err != nil {
		return err
	}
	sort.Sort(logIDsSlice(ids))

	deleted := old[:0] // Reuse the old slice for deleted IDs.
	var added []int64

	i, j := 0, 0
	for i < len(old) || j < len(ids) {
		if i < len(old) && (j >= len(ids) || old[i] < ids[j]) {
			deleted = append(deleted, old[i])
			i++
		} else if j < len(ids) && (i >= len(old) || ids[j] < old[i]) {
			added = append(added, ids[j])
			j++
		} else { // Both IDs are the same.
			i++
			j++
		}
	}

	t.ids = ids
	return added, deleted, nil
}

// getActiveLogIDs returns IDs of all currently active logs.
func (t *Tracker) getActiveLogIDs(ctx context.Context) ([]int64, error) {
	tx, err := t.reg.LogStorage.Snapshot(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction: %v", err)
	}
	defer tx.Close()

	logIDs, err := tx.GetActiveLogIDs(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get active logIDs: %v", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit: %v", err)
	}
	return logIDs, nil
}

// logIDsSlice is used for sorting int64 log IDs.
type logIDsSlice []int64

func (l logIDsSlice) Len() int      { return len(l) }
func (l logIDsSlice) Less(i, j int) { return l[i] < l[j] }
func (l logIDsSlice) Swap(i, j int) { l[i], l[j] = l[j], l[i] }
