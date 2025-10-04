// Copyright (c) 2025 Pano Operations Ltd
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at panoptisDev.com/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package cache

import (
	"testing"

	"github.com/panoptisDev/carmen/go/backend/index"
	"github.com/panoptisDev/carmen/go/backend/index/memory"
	"github.com/panoptisDev/carmen/go/common"
)

var (
	address1 = common.Address{0x01}
	address2 = common.Address{0x02}
	address3 = common.Address{0x03}
	address4 = common.Address{0x04}
)

func TestIndexCacheFilled(t *testing.T) {
	index := NewIndex[common.Address, uint32](memory.NewIndex[common.Address, uint32](common.AddressSerializer{}), 3)

	_, _ = index.GetOrAdd(address1)
	val, exists := index.cache.Get(address1)
	if !exists || val != 0 {
		t.Errorf("Value is not propagated in cahce")
	}
}

func TestIndexCacheEviction(t *testing.T) {
	index := NewIndex[common.Address, uint32](memory.NewIndex[common.Address, uint32](common.AddressSerializer{}), 3)

	_, _ = index.GetOrAdd(address1)
	_, _ = index.GetOrAdd(address2)
	_, _ = index.GetOrAdd(address3)
	_, _ = index.GetOrAdd(address4)

	// fist item evicted from cache
	_, exists := index.cache.Get(address1)
	if exists {
		t.Errorf("Value is not evicted from cahce")
	}

	// it returns value in cache
	_ = index.Contains(address1)
	val, exists := index.cache.Get(address1)
	if !exists || val != 0 {
		t.Errorf("Value is not in cahce")
	}
}

var ErrNotFound = index.ErrNotFound

func TestNonExistingValuesAreNotCached(t *testing.T) {
	index := NewIndex[common.Address, uint32](memory.NewIndex[common.Address, uint32](common.AddressSerializer{}), 3)
	_, err := index.Get(address1)
	if err != ErrNotFound {
		t.Errorf("Address 1 should not exist")
	}
	_, err = index.Get(address1)
	if err != ErrNotFound {
		t.Errorf("Address 1 should still not exist")
	}
}
