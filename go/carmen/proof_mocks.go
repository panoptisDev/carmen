// Copyright (c) 2025 Pano Operations Ltd
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at panoptisDev.com/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

// Package carmen is a generated GoMock package.
package carmen

import (
	reflect "reflect"

	gomock "go.uber.org/mock/gomock"
)

// MockWitnessProof is a mock of WitnessProof interface.
type MockWitnessProof struct {
	ctrl     *gomock.Controller
	recorder *MockWitnessProofMockRecorder
	isgomock struct{}
}

// MockWitnessProofMockRecorder is the mock recorder for MockWitnessProof.
type MockWitnessProofMockRecorder struct {
	mock *MockWitnessProof
}

// NewMockWitnessProof creates a new mock instance.
func NewMockWitnessProof(ctrl *gomock.Controller) *MockWitnessProof {
	mock := &MockWitnessProof{ctrl: ctrl}
	mock.recorder = &MockWitnessProofMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockWitnessProof) EXPECT() *MockWitnessProofMockRecorder {
	return m.recorder
}

// AllAddressesEmpty mocks base method.
func (m *MockWitnessProof) AllAddressesEmpty(root Hash, from, to Address) (Tribool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AllAddressesEmpty", root, from, to)
	ret0, _ := ret[0].(Tribool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AllAddressesEmpty indicates an expected call of AllAddressesEmpty.
func (mr *MockWitnessProofMockRecorder) AllAddressesEmpty(root, from, to any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AllAddressesEmpty", reflect.TypeOf((*MockWitnessProof)(nil).AllAddressesEmpty), root, from, to)
}

// AllStatesZero mocks base method.
func (m *MockWitnessProof) AllStatesZero(root Hash, address Address, from, to Key) (Tribool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AllStatesZero", root, address, from, to)
	ret0, _ := ret[0].(Tribool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AllStatesZero indicates an expected call of AllStatesZero.
func (mr *MockWitnessProofMockRecorder) AllStatesZero(root, address, from, to any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AllStatesZero", reflect.TypeOf((*MockWitnessProof)(nil).AllStatesZero), root, address, from, to)
}

// Extract mocks base method.
func (m *MockWitnessProof) Extract(root Hash, address Address, keys ...Key) (WitnessProof, bool) {
	m.ctrl.T.Helper()
	varargs := []any{root, address}
	for _, a := range keys {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Extract", varargs...)
	ret0, _ := ret[0].(WitnessProof)
	ret1, _ := ret[1].(bool)
	return ret0, ret1
}

// Extract indicates an expected call of Extract.
func (mr *MockWitnessProofMockRecorder) Extract(root, address any, keys ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{root, address}, keys...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Extract", reflect.TypeOf((*MockWitnessProof)(nil).Extract), varargs...)
}

// GetAccountElements mocks base method.
func (m *MockWitnessProof) GetAccountElements(root Hash, address Address) ([]Bytes, Hash, bool) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetAccountElements", root, address)
	ret0, _ := ret[0].([]Bytes)
	ret1, _ := ret[1].(Hash)
	ret2, _ := ret[2].(bool)
	return ret0, ret1, ret2
}

// GetAccountElements indicates an expected call of GetAccountElements.
func (mr *MockWitnessProofMockRecorder) GetAccountElements(root, address any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetAccountElements", reflect.TypeOf((*MockWitnessProof)(nil).GetAccountElements), root, address)
}

// GetBalance mocks base method.
func (m *MockWitnessProof) GetBalance(root Hash, address Address) (Amount, bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetBalance", root, address)
	ret0, _ := ret[0].(Amount)
	ret1, _ := ret[1].(bool)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// GetBalance indicates an expected call of GetBalance.
func (mr *MockWitnessProofMockRecorder) GetBalance(root, address any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetBalance", reflect.TypeOf((*MockWitnessProof)(nil).GetBalance), root, address)
}

// GetCodeHash mocks base method.
func (m *MockWitnessProof) GetCodeHash(root Hash, address Address) (Hash, bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetCodeHash", root, address)
	ret0, _ := ret[0].(Hash)
	ret1, _ := ret[1].(bool)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// GetCodeHash indicates an expected call of GetCodeHash.
func (mr *MockWitnessProofMockRecorder) GetCodeHash(root, address any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetCodeHash", reflect.TypeOf((*MockWitnessProof)(nil).GetCodeHash), root, address)
}

// GetElements mocks base method.
func (m *MockWitnessProof) GetElements() []Bytes {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetElements")
	ret0, _ := ret[0].([]Bytes)
	return ret0
}

// GetElements indicates an expected call of GetElements.
func (mr *MockWitnessProofMockRecorder) GetElements() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetElements", reflect.TypeOf((*MockWitnessProof)(nil).GetElements))
}

// GetNonce mocks base method.
func (m *MockWitnessProof) GetNonce(root Hash, address Address) (uint64, bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetNonce", root, address)
	ret0, _ := ret[0].(uint64)
	ret1, _ := ret[1].(bool)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// GetNonce indicates an expected call of GetNonce.
func (mr *MockWitnessProofMockRecorder) GetNonce(root, address any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetNonce", reflect.TypeOf((*MockWitnessProof)(nil).GetNonce), root, address)
}

// GetState mocks base method.
func (m *MockWitnessProof) GetState(root Hash, address Address, key Key) (Value, bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetState", root, address, key)
	ret0, _ := ret[0].(Value)
	ret1, _ := ret[1].(bool)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// GetState indicates an expected call of GetState.
func (mr *MockWitnessProofMockRecorder) GetState(root, address, key any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetState", reflect.TypeOf((*MockWitnessProof)(nil).GetState), root, address, key)
}

// GetStorageElements mocks base method.
func (m *MockWitnessProof) GetStorageElements(root Hash, address Address, key Key) ([]Bytes, bool) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetStorageElements", root, address, key)
	ret0, _ := ret[0].([]Bytes)
	ret1, _ := ret[1].(bool)
	return ret0, ret1
}

// GetStorageElements indicates an expected call of GetStorageElements.
func (mr *MockWitnessProofMockRecorder) GetStorageElements(root, address, key any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetStorageElements", reflect.TypeOf((*MockWitnessProof)(nil).GetStorageElements), root, address, key)
}

// IsValid mocks base method.
func (m *MockWitnessProof) IsValid() bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IsValid")
	ret0, _ := ret[0].(bool)
	return ret0
}

// IsValid indicates an expected call of IsValid.
func (mr *MockWitnessProofMockRecorder) IsValid() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IsValid", reflect.TypeOf((*MockWitnessProof)(nil).IsValid))
}
