// Copyright 2016 Google LLC. All Rights Reserved.
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

package merkle

import (
	"errors"

	"github.com/google/trillian/merkle/proof"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CalcInclusionProofNodeAddresses returns the tree node IDs needed to build an
// inclusion proof for a specified tree size and leaf index. All the returned
// nodes represent complete subtrees in the tree of this size or above.
//
// Use Rehash function to compose the proof after the node hashes are fetched.
//
// TODO(pavelkalinnikov): Deprecate this file, use proof/proof.go directly.
func CalcInclusionProofNodeAddresses(size, index int64) (proof.Nodes, error) {
	if size < 1 {
		return proof.Nodes{}, status.Errorf(codes.InvalidArgument, "invalid parameter for inclusion proof: size %d < 1", size)
	}
	if index >= size {
		return proof.Nodes{}, status.Errorf(codes.InvalidArgument, "invalid parameter for inclusion proof: index %d is >= size %d", index, size)
	}
	if index < 0 {
		return proof.Nodes{}, status.Errorf(codes.InvalidArgument, "invalid parameter for inclusion proof: index %d is < 0", index)
	}
	return proof.Inclusion(uint64(index), uint64(size)), nil
}

// CalcConsistencyProofNodeAddresses returns the tree node IDs needed to build
// a consistency proof between two specified tree sizes. All the returned nodes
// represent complete subtrees in the tree of size2 or above.
//
// Use Rehash function to compose the proof after the node hashes are fetched.
func CalcConsistencyProofNodeAddresses(size1, size2 int64) (proof.Nodes, error) {
	if size1 < 1 {
		return proof.Nodes{}, status.Errorf(codes.InvalidArgument, "invalid parameter for consistency proof: size1 %d < 1", size1)
	}
	if size2 < 1 {
		return proof.Nodes{}, status.Errorf(codes.InvalidArgument, "invalid parameter for consistency proof: size2 %d < 1", size2)
	}
	if size1 > size2 {
		return proof.Nodes{}, status.Errorf(codes.InvalidArgument, "invalid parameter for consistency proof: size1 %d > size2 %d", size1, size2)
	}
	return proof.Consistency(uint64(size1), uint64(size2)), nil
}

// Rehash computes the proof based on the slice of NodeFetch structs, and the
// corresponding hashes of these nodes. The slices must be of the same length.
// The hc parameter computes node's hash based on hashes of its children.
//
// Warning: The passed-in slice of hashes can be modified in-place.
func Rehash(h [][]byte, n proof.Nodes, hc func(left, right []byte) []byte) ([][]byte, error) {
	if len(h) != len(n.IDs) {
		return nil, errors.New("slice lengths mismatch")
	}
	cursor := 0
	// Scan the list of node hashes, and store the rehashed list in-place.
	// Invariant: cursor <= i, and h[:cursor] contains all the hashes of the
	// rehashed list after scanning h up to index i-1.
	for i, ln := 0, len(h); i < ln; i, cursor = i+1, cursor+1 {
		hash := h[i]
		if i >= n.Begin && i < n.End {
			// Scan the block of node hashes that need rehashing.
			for i++; i < n.End; i++ {
				hash = hc(h[i], hash)
			}
			i--
		}
		h[cursor] = hash
	}
	return h[:cursor], nil
}
