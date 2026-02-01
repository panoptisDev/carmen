// Copyright (c) 2025 Pano Operations Ltd
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at panoptisdev.com/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package memory

import (
	"strings"

	"github.com/panoptisDev/carmen/go/database/vt/memory/trie"
	"github.com/panoptisDev/carmen/go/database/vt/reference"
	"github.com/panoptisDev/carmen/go/state"
)

// NewState creates a new, empty in-memory state instance.
func NewState(params state.Parameters) (state.State, error) {
	config := trie.TrieConfig{
		ParallelCommit: !strings.HasSuffix(string(params.Variant), "-seq"),
	}
	return reference.NewStateUsing(trie.NewTrie(config)), nil
}
