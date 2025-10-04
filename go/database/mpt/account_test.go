// Copyright (c) 2025 Pano Operations Ltd
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at panoptisDev.com/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package mpt

import (
	"testing"

	"github.com/panoptisDev/carmen/go/common"
	"github.com/panoptisDev/carmen/go/common/amount"
)

func TestAccountInfo_EncodingAndDecoding(t *testing.T) {
	infos := []AccountInfo{
		{},
		{common.Nonce{1, 2, 3}, amount.New(456), common.Hash{7, 8, 9}},
	}

	encoder := AccountInfoEncoder{}
	buffer := make([]byte, encoder.GetEncodedSize())
	for _, info := range infos {
		encoder.Store(buffer[:], &info)
		restored := AccountInfo{}
		if encoder.Load(buffer[:], &restored); restored != info {
			t.Fatalf("failed to decode info %v: got %v", info, restored)
		}
	}
}
