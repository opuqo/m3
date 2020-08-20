// Code generated by MockGen. DO NOT EDIT.
// Source: /Users/notbdu/go/src/github.com/m3db/m3/src/m3ninx/search/types.go

// Package search is a generated GoMock package.
package search

import (
	gomock "github.com/golang/mock/gomock"
	doc "github.com/m3db/m3/src/m3ninx/doc"
	querypb "github.com/m3db/m3/src/m3ninx/generated/proto/querypb"
	index "github.com/m3db/m3/src/m3ninx/index"
	postings "github.com/m3db/m3/src/m3ninx/postings"
	reflect "reflect"
)

// MockExecutor is a mock of Executor interface
type MockExecutor struct {
	ctrl     *gomock.Controller
	recorder *MockExecutorMockRecorder
}

// MockExecutorMockRecorder is the mock recorder for MockExecutor
type MockExecutorMockRecorder struct {
	mock *MockExecutor
}

// NewMockExecutor creates a new mock instance
func NewMockExecutor(ctrl *gomock.Controller) *MockExecutor {
	mock := &MockExecutor{ctrl: ctrl}
	mock.recorder = &MockExecutorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockExecutor) EXPECT() *MockExecutorMockRecorder {
	return m.recorder
}

// Execute mocks base method
func (m *MockExecutor) Execute(q Query) (doc.Iterator, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Execute", q)
	ret0, _ := ret[0].(doc.Iterator)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Execute indicates an expected call of Execute
func (mr *MockExecutorMockRecorder) Execute(q interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Execute", reflect.TypeOf((*MockExecutor)(nil).Execute), q)
}

// Close mocks base method
func (m *MockExecutor) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close
func (mr *MockExecutorMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockExecutor)(nil).Close))
}

// MockQuery is a mock of Query interface
type MockQuery struct {
	ctrl     *gomock.Controller
	recorder *MockQueryMockRecorder
}

// MockQueryMockRecorder is the mock recorder for MockQuery
type MockQueryMockRecorder struct {
	mock *MockQuery
}

// NewMockQuery creates a new mock instance
func NewMockQuery(ctrl *gomock.Controller) *MockQuery {
	mock := &MockQuery{ctrl: ctrl}
	mock.recorder = &MockQueryMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockQuery) EXPECT() *MockQueryMockRecorder {
	return m.recorder
}

// String mocks base method
func (m *MockQuery) String() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "String")
	ret0, _ := ret[0].(string)
	return ret0
}

// String indicates an expected call of String
func (mr *MockQueryMockRecorder) String() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "String", reflect.TypeOf((*MockQuery)(nil).String))
}

// Searcher mocks base method
func (m *MockQuery) Searcher() (Searcher, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Searcher")
	ret0, _ := ret[0].(Searcher)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Searcher indicates an expected call of Searcher
func (mr *MockQueryMockRecorder) Searcher() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Searcher", reflect.TypeOf((*MockQuery)(nil).Searcher))
}

// Equal mocks base method
func (m *MockQuery) Equal(q Query) bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Equal", q)
	ret0, _ := ret[0].(bool)
	return ret0
}

// Equal indicates an expected call of Equal
func (mr *MockQueryMockRecorder) Equal(q interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Equal", reflect.TypeOf((*MockQuery)(nil).Equal), q)
}

// ToProto mocks base method
func (m *MockQuery) ToProto() *querypb.Query {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ToProto")
	ret0, _ := ret[0].(*querypb.Query)
	return ret0
}

// ToProto indicates an expected call of ToProto
func (mr *MockQueryMockRecorder) ToProto() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ToProto", reflect.TypeOf((*MockQuery)(nil).ToProto))
}

// MockSearcher is a mock of Searcher interface
type MockSearcher struct {
	ctrl     *gomock.Controller
	recorder *MockSearcherMockRecorder
}

// MockSearcherMockRecorder is the mock recorder for MockSearcher
type MockSearcherMockRecorder struct {
	mock *MockSearcher
}

// NewMockSearcher creates a new mock instance
func NewMockSearcher(ctrl *gomock.Controller) *MockSearcher {
	mock := &MockSearcher{ctrl: ctrl}
	mock.recorder = &MockSearcherMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockSearcher) EXPECT() *MockSearcherMockRecorder {
	return m.recorder
}

// Search mocks base method
func (m *MockSearcher) Search(arg0 index.Reader) (postings.List, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Search", arg0)
	ret0, _ := ret[0].(postings.List)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Search indicates an expected call of Search
func (mr *MockSearcherMockRecorder) Search(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Search", reflect.TypeOf((*MockSearcher)(nil).Search), arg0)
}
