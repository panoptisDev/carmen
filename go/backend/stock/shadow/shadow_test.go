// Copyright (c) 2025 Pano Operations Ltd
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at panoptisDev.com/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package shadow

import (
	"errors"
	"testing"

	"github.com/panoptisDev/carmen/go/backend/stock"
	"github.com/panoptisDev/carmen/go/backend/stock/file"
	"github.com/panoptisDev/carmen/go/backend/stock/memory"
)

func TestShadowStock(t *testing.T) {
	stock.RunStockTests(t, stock.NamedStockFactory{
		ImplementationName: "shadow",
		Open:               openShadowStock,
	})
}

func openShadowStock(t *testing.T, directory string) (stock.Stock[int, int], error) {
	a, errA := file.OpenStock[int, int](stock.IntEncoder{}, directory+"/A")
	b, errB := memory.OpenStock[int, int](stock.IntEncoder{}, directory+"/B")
	return MakeShadowStock(a, b), errors.Join(errA, errB)
}
