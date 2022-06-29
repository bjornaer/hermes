package btree_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/bjornaer/hermes/internal/disk/btree"
	"github.com/bjornaer/hermes/internal/disk/pair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type UnitTestSuite struct {
	suite.Suite
	totalElements int
	path          string
	tree          *btree.Btree[string]
}

func (s *UnitTestSuite) SetupTest() {
	path := "./db/test.db"
	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.Mkdir("./db", os.ModePerm)
	}
	s.path = "./db/"
	tree, err := btree.InitializeBtree[string](path)
	if err != nil {
		s.T().Error(err)
	}
	s.tree = tree
	s.totalElements = 250

}

func (s *UnitTestSuite) BeforeTest(suiteName, testName string) {
	for i := 1; i <= s.totalElements; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		s.tree.Insert(pair.NewPair(key, value))
	}
}

func (s *UnitTestSuite) AfterTest(suiteName, testName string) {
	if _, err := os.Stat(s.path); err == nil {
		// path/to/whatever exists
		err := os.RemoveAll(s.path)
		if err != nil {
			panic(err)
		}
	}
}

func BtreeInsert(s *UnitTestSuite) {
	for i := 1; i <= 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		err := s.tree.Insert(pair.NewPair(key, value))
		assert.Nil(s.T(), err)
	}
	// tree.root.PrintTree()
}

func BtreeGet(s *UnitTestSuite) {
	for i := 1; i <= s.totalElements; i++ {
		key := fmt.Sprintf("key-%d", i)
		expected := fmt.Sprintf("value-%d", i)
		value, _, found, err := s.tree.Get(key)
		if err != nil {
			s.T().Error(err)
		}
		assert.True(s.T(), found)
		assert.Equal(s.T(), expected, value)
	}
}

func BtreeGetInexistent(s *UnitTestSuite) {
	for i := s.totalElements + 1; i <= s.totalElements+1+1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		_, _, found, err := s.tree.Get(key)
		if err != nil {
			s.T().Error(err)
		}
		assert.False(s.T(), found, "values should not be found")
	}
}

func BtreeIterateF(s *UnitTestSuite) {
	counter := 0
	err := s.tree.Iterate(func(k string, v string, t time.Time) error {
		counter += 1
		assert.NotZero(s.T(), k)
		assert.NotZero(s.T(), v)
		assert.NotZero(s.T(), t)
		return nil
	})
	if err != nil {
		s.T().Error(err)
	}
	expected, err := s.tree.Count()
	if err != nil {
		s.T().Error(err)
	}
	assert.Equal(s.T(), expected, counter)
}

func (s *UnitTestSuite) Test_TableTest() {

	type testCase struct {
		name   string
		treeFn func(s *UnitTestSuite)
	}

	testCases := []testCase{
		{
			name:   "Insert Multiple",
			treeFn: BtreeInsert,
		},
		{
			name:   "Get Each Value",
			treeFn: BtreeInsert,
		},
		{
			name:   "Get Non Existent Value",
			treeFn: BtreeInsert,
		},
		{
			name:   "Iterate Over Whole Tree",
			treeFn: BtreeIterateF,
		},
	}

	for _, testCase := range testCases {

		s.Run(testCase.name, func() {
			testCase.treeFn(s)
		})
	}
}

func TestUnitTestSuite(t *testing.T) {
	suite.Run(t, new(UnitTestSuite))
}
