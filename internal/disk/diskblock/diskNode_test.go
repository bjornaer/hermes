package diskblock_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/bjornaer/hermes/internal/disk/diskblock"
	"github.com/bjornaer/hermes/internal/disk/pair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type DiskNodeUnitTestSuite struct {
	suite.Suite
	totalElements int
	path          string
	blockservice  *diskblock.BlockService
}

func raiseErrorDuringTest(s *DiskNodeUnitTestSuite, err error) {
	if err != nil {
		s.T().Error(err)
	}
}

func (s *DiskNodeUnitTestSuite) SetupTest() {
	path := "./db/node_test.db"
	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.Mkdir("./db", os.ModePerm)
	}
	if _, err := os.Stat(path); err == nil {
		// path/to/whatever exists
		err := os.Remove(path)
		if err != nil {
			panic(err)
		}
	}
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}

	s.path = "./db"
	s.blockservice = diskblock.NewBlockService(file)

}

func (s *DiskNodeUnitTestSuite) BeforeTest(suiteName, testName string) {

}

func (s *DiskNodeUnitTestSuite) AfterTest(suiteName, testName string) {
	if _, err := os.Stat(s.path); err == nil {
		// path/to/whatever exists
		err := os.RemoveAll(s.path)
		if err != nil {
			panic(err)
		}
	}
}

func ShouldAddElement(s *DiskNodeUnitTestSuite) {
	elements := make([]*pair.Pairs, 3)
	elements[0] = pair.NewPair("hola", "amigos")
	elements[1] = pair.NewPair("foo", "bar")
	elements[2] = pair.NewPair("gooz", "bumps")
	n, err := diskblock.NewLeafNode(elements, s.blockservice)
	raiseErrorDuringTest(s, err)
	addedElement := pair.NewPair("added", "please check")
	n.AddElement(addedElement)
	expected := []*pair.Pairs{addedElement, elements[0], elements[1], elements[2]}
	assert.Equal(s.T(), n.GetElements(), expected)
}

func ShouldAddElementInOrder(s *DiskNodeUnitTestSuite) {
	first := pair.NewPair("first", "value")
	second := pair.NewPair("second", "value")
	n, err := diskblock.NewLeafNode([]*pair.Pairs{first}, s.blockservice)
	raiseErrorDuringTest(s, err)
	n.AddElement(second)
	expected := []*pair.Pairs{first, second}
	assert.Equal(s.T(), n.GetElements(), expected)

	third := pair.NewPair("third", "value")
	n, err = diskblock.NewLeafNode([]*pair.Pairs{first, second, third}, s.blockservice)
	raiseErrorDuringTest(s, err)
	fourth := pair.NewPair("fourth", "value")
	n.AddElement(fourth)
	expected = []*pair.Pairs{first, fourth, second, third}
	assert.Equal(s.T(), n.GetElements(), expected)

}

func ShouldBeLeaf(s *DiskNodeUnitTestSuite) {
	child1, err := diskblock.NewLeafNode([]*pair.Pairs{pair.NewPair("first", "value"),
		pair.NewPair("second", "value")}, s.blockservice)
	raiseErrorDuringTest(s, err)
	child2, err := diskblock.NewLeafNode([]*pair.Pairs{pair.NewPair("third", "value"),
		pair.NewPair("forth", "value")}, s.blockservice)
	raiseErrorDuringTest(s, err)
	n, err := diskblock.NewNodeWithChildren([]*pair.Pairs{pair.NewPair("fifth", "value"),
		pair.NewPair("sixth", "value")}, []uint64{child1.BlockID, child2.BlockID}, s.blockservice)
	raiseErrorDuringTest(s, err)
	assert.False(s.T(), n.IsLeaf())

	child1, err = diskblock.NewLeafNode(nil, s.blockservice)
	raiseErrorDuringTest(s, err)
	child2, err = diskblock.NewLeafNode(nil, s.blockservice)
	raiseErrorDuringTest(s, err)
	n, err = diskblock.NewNodeWithChildren([]*pair.Pairs{pair.NewPair("first", "value"),
		pair.NewPair("second", "value")}, nil, s.blockservice)
	raiseErrorDuringTest(s, err)
	assert.True(s.T(), n.IsLeaf())

	n, err = diskblock.NewLeafNode([]*pair.Pairs{pair.NewPair("first", "value"),
		pair.NewPair("second", "value")}, s.blockservice)
	raiseErrorDuringTest(s, err)
	assert.True(s.T(), n.IsLeaf())
}

func HasOverFlown(s *DiskNodeUnitTestSuite) {
	elements := make([]*pair.Pairs, s.blockservice.GetMaxLeafSize()+1)
	for i := 0; i < s.blockservice.GetMaxLeafSize()+1; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		elements[i] = pair.NewPair(key, value)
	}
	n, err := diskblock.NewLeafNode(elements, s.blockservice)
	raiseErrorDuringTest(s, err)
	assert.True(s.T(), n.HasOverFlown(), "Should return true as node has overflown")

	n, err = diskblock.NewLeafNode([]*pair.Pairs{pair.NewPair("first", "value"), pair.NewPair("fourth", "value"),
		pair.NewPair("second", "value"), pair.NewPair("third", "value")}, s.blockservice)
	raiseErrorDuringTest(s, err)
	assert.False(s.T(), n.HasOverFlown(), "Should return false as node has not overflown")

}

func SplitLeafNode(s *DiskNodeUnitTestSuite) {
	nodePairs := []*pair.Pairs{pair.NewPair("first", "value"), pair.NewPair("fourth", "value"), pair.NewPair("second", "value"), pair.NewPair("third", "value")}
	n, err := diskblock.NewLeafNode(nodePairs, s.blockservice)
	raiseErrorDuringTest(s, err)
	poppedUpMiddleElement, leftChild, rightChild, err := n.SplitLeafNode()
	raiseErrorDuringTest(s, err)
	assert.Equal(s.T(), poppedUpMiddleElement.Key, "second")
	assert.Equal(s.T(), leftChild.GetElementAtIndex(1).Key, "fourth", "Should have proper value at leftchild")
	assert.Equal(s.T(), rightChild.GetElementAtIndex(0).Key, "third", "Should have proper value at rightchild")
}

func SplitNonLeafNode(s *DiskNodeUnitTestSuite) {
	child1, err := diskblock.NewLeafNode([]*pair.Pairs{pair.NewPair("1first", "value"),
		pair.NewPair("1fourth", "value"), pair.NewPair("1second", "value"), pair.NewPair("1third", "value")}, s.blockservice)
	raiseErrorDuringTest(s, err)
	child2, err := diskblock.NewLeafNode([]*pair.Pairs{pair.NewPair("2first", "value"),
		pair.NewPair("2fourth", "value"), pair.NewPair("2second", "value"), pair.NewPair("2third", "value")}, s.blockservice)
	raiseErrorDuringTest(s, err)
	child3, err := diskblock.NewLeafNode([]*pair.Pairs{pair.NewPair("3first", "value"),
		pair.NewPair("3fourth", "value"), pair.NewPair("3second", "value"), pair.NewPair("3third", "value")}, s.blockservice)
	raiseErrorDuringTest(s, err)
	child4, err := diskblock.NewLeafNode([]*pair.Pairs{pair.NewPair("4first", "value"),
		pair.NewPair("4fourth", "value"), pair.NewPair("4second", "value"), pair.NewPair("4third", "value")}, s.blockservice)
	raiseErrorDuringTest(s, err)
	child5, err := diskblock.NewLeafNode([]*pair.Pairs{pair.NewPair("5first", "value"),
		pair.NewPair("5fourth", "value"), pair.NewPair("5second", "value"), pair.NewPair("5third", "value")}, s.blockservice)
	raiseErrorDuringTest(s, err)

	n, err := diskblock.NewNodeWithChildren([]*pair.Pairs{pair.NewPair("nfirst", "value"),
		pair.NewPair("nfourth", "value"), pair.NewPair("nsecond", "value"), pair.NewPair("nthird", "value")},
		[]uint64{child1.BlockID, child2.BlockID, child3.BlockID,
			child4.BlockID, child5.BlockID}, s.blockservice)
	raiseErrorDuringTest(s, err)
	poppedUpMiddleElement, leftChild, rightChild, err := n.SplitNonLeafNode()
	raiseErrorDuringTest(s, err)
	assert.Equal(s.T(), poppedUpMiddleElement.Key, "nsecond")

	childToBeTested, err := leftChild.GetChildAtIndex(2)
	raiseErrorDuringTest(s, err)
	assert.Equal(s.T(), childToBeTested.GetElementAtIndex(2).Key, "3second")

	childToBeTested, err = leftChild.GetChildAtIndex(1)
	raiseErrorDuringTest(s, err)
	assert.Equal(s.T(), childToBeTested.GetElementAtIndex(3).Key, "2third")

	childToBeTested, err = rightChild.GetChildAtIndex(1)
	raiseErrorDuringTest(s, err)
	assert.Equal(s.T(), childToBeTested.GetElementAtIndex(3).Key, "5third")
}

func AddPoppedupElement(s *DiskNodeUnitTestSuite) {

	child1OfParent, err := diskblock.NewLeafNode([]*pair.Pairs{pair.NewPair("1first", "value"),
		pair.NewPair("1fourth", "value"), pair.NewPair("1second", "value"), pair.NewPair("1third", "value")}, s.blockservice)
	raiseErrorDuringTest(s, err)
	child2OfParent, err := diskblock.NewLeafNode([]*pair.Pairs{pair.NewPair("2first", "value"),
		pair.NewPair("2fourth", "value"), pair.NewPair("2second", "value"), pair.NewPair("2third", "value")}, s.blockservice)
	raiseErrorDuringTest(s, err)
	parentNode, err := diskblock.NewNodeWithChildren([]*pair.Pairs{pair.NewPair("parentfirst", "value")}, []uint64{child1OfParent.BlockID,
		child2OfParent.BlockID}, s.blockservice)
	child3, err := diskblock.NewLeafNode([]*pair.Pairs{pair.NewPair("3first", "value"),
		pair.NewPair("3fourth", "value"), pair.NewPair("3second", "value"), pair.NewPair("3third", "value")}, s.blockservice)
	raiseErrorDuringTest(s, err)
	child4, err := diskblock.NewLeafNode([]*pair.Pairs{pair.NewPair("4first", "value"),
		pair.NewPair("4fourth", "value"), pair.NewPair("4second", "value"), pair.NewPair("4third", "value")}, s.blockservice)
	raiseErrorDuringTest(s, err)
	parentNode.AddPoppedUpElementIntoCurrentNodeAndUpdateWithNewChildren(pair.NewPair("popfirst", "value"), child3, child4)

	child, err := parentNode.GetChildAtIndex(0)
	raiseErrorDuringTest(s, err)
	assert.Equal(s.T(), child.GetElementAtIndex(0).Key, "1first")

	child, err = parentNode.GetChildAtIndex(2)
	raiseErrorDuringTest(s, err)
	assert.Equal(s.T(), child.GetElementAtIndex(0).Key, "4first")
}

func (s *DiskNodeUnitTestSuite) Test_DiskNodeTableTest() {
	type testCase struct {
		name         string
		disckblockFn func(s *DiskNodeUnitTestSuite)
	}
	var testCases = []testCase{
		{
			name:         "Should add elements",
			disckblockFn: ShouldAddElement,
		},
		{
			name:         "Should add elements in right order",
			disckblockFn: ShouldAddElementInOrder,
		},
		{
			name:         "Assert IsLeaf is accurate",
			disckblockFn: ShouldBeLeaf,
		},
		{
			name:         "Node Overflow",
			disckblockFn: HasOverFlown,
		},
		{
			name:         "Split Leaf Node",
			disckblockFn: SplitLeafNode,
		},
		{
			name:         "Split Non Leaf Node",
			disckblockFn: SplitNonLeafNode,
		},
		{
			name:         "Re Add Popped element",
			disckblockFn: AddPoppedupElement,
		},
	}

	for _, testCase := range testCases {

		s.Run(testCase.name, func() {
			testCase.disckblockFn(s)
		})
	}
}
func TestDiskNodeUnitTestSuite(t *testing.T) {
	suite.Run(t, new(DiskNodeUnitTestSuite))
}
