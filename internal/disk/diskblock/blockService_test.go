package diskblock_test

import (
	"os"
	"testing"

	"github.com/bjornaer/hermes/internal/disk/diskblock"
	"github.com/bjornaer/hermes/internal/disk/pair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type UnitTestSuite struct {
	suite.Suite
	totalElements int
	path          string
	blockservice  *diskblock.BlockService
}

func (s *UnitTestSuite) SetupTest() {
	path := "./db/block_test.db"
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

func (s *UnitTestSuite) BeforeTest(suiteName, testName string) {

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

func ShouldGetNegativeIfBlockNotPresent(s *UnitTestSuite) {
	latestBlockID, _ := s.blockservice.GetLatestBlockID()
	assert.Equal(s.T(), int64(-1), latestBlockID)
}

func ShouldSuccessfullyInitializeNewBlock(s *UnitTestSuite) {
	block, err := s.blockservice.GetRootBlock()
	if err != nil {
		s.T().Error(err)
	}
	assert.Zero(s.T(), block.Id, "Root Block id should be zero")
	assert.Zero(s.T(), block.CurrentLeafSize, "Block leaf size should be zero")
}

func ShouldSaveNewBlockOnDisk(s *UnitTestSuite) {
	block, err := s.blockservice.GetRootBlock()
	if err != nil {
		s.T().Error(err)
	}
	assert.Zero(s.T(), block.Id, "Root Block id should be zero")
	assert.Zero(s.T(), block.CurrentLeafSize, "Block leaf size should be zero")

	elements := make([]*pair.Pairs, 3)
	elements[0] = pair.NewPair("hola", "amigos")
	elements[1] = pair.NewPair("foo", "bar")
	elements[2] = pair.NewPair("gooz", "bumps")
	block.SetData(elements)
	err = s.blockservice.WriteBlockToDisk(block)
	if err != nil {
		s.T().Error(err)
	}

	block, err = s.blockservice.GetRootBlock()
	if err != nil {
		s.T().Error(err)
	}
	assert.NotZero(s.T(), block.DataSet, "Length of data field should not be zero")
}

func ShouldConvertPairToAndFromBytes(s *UnitTestSuite) {
	p := pair.NewPair("Hola  ", "Amigos")
	pairBytes := pair.ConvertPairsToBytes(p)
	convertedPair := pair.ConvertBytesToPair(pairBytes)
	assert.Equal(s.T(), p.KeyLen, convertedPair.KeyLen, "Key length should match")
	assert.Equal(s.T(), p.ValueLen, convertedPair.ValueLen, "Value length should match")
	assert.Equal(s.T(), p.Key, convertedPair.Key, "Key should match")
	assert.Equal(s.T(), p.Value, convertedPair.Value, "Value should match")
}

func ShouldConvertBlockToAndFromBytes(s *UnitTestSuite) {
	block := &diskblock.DiskBlock{}
	block.SetChildren([]uint64{2, 3, 4, 6})

	elements := make([]*pair.Pairs, 3)
	elements[0] = pair.NewPair("hola", "amigos")
	elements[1] = pair.NewPair("foo", "bar")
	elements[2] = pair.NewPair("gooz", "bumps")
	block.SetData(elements)
	blockBuffer := s.blockservice.GetBufferFromBlock(block)
	convertedBlock := s.blockservice.GetBlockFromBuffer(blockBuffer)

	assert.Equal(s.T(), 4, int(convertedBlock.ChildrenBlockIds[2]))
	assert.Equal(s.T(), len(convertedBlock.DataSet), len(block.DataSet), "Length of blocks should be same")
	assert.Equal(s.T(), convertedBlock.DataSet[1].Key, block.DataSet[1].Key, "Keys should match")
	assert.Equal(s.T(), convertedBlock.DataSet[2].Value, block.DataSet[2].Value, "Values should match")
}

func ShouldConvertToAndFromDiskNode(s *UnitTestSuite) {
	bs := s.blockservice
	node := &diskblock.DiskNode{}
	node.BlockID = 55
	elements := make([]*pair.Pairs, 3)
	elements[0] = pair.NewPair("hola", "amigos")
	node.Keys = elements
	node.ChildrenBlockIDs = []uint64{1000, 10001}
	block := bs.ConvertDiskNodeToBlock(node)
	assert.Equal(s.T(), 55, int(block.Id), "Should have same block id as node block id")
	assert.Equal(s.T(), 10001, int(block.ChildrenBlockIds[1]), "Block ids should match")

	nodeFromBlock := bs.ConvertBlockToDiskNode(block)

	assert.Equal(s.T(), nodeFromBlock.BlockID, node.BlockID)

	assert.Equal(s.T(), 1000, int(nodeFromBlock.ChildrenBlockIDs[0]))
	assert.Equal(s.T(), "hola", nodeFromBlock.Keys[0].Key)
}

func (s *UnitTestSuite) Test_TableTest() {
	type testCase struct {
		name         string
		disckblockFn func(s *UnitTestSuite)
	}
	testCases := []testCase{
		{
			name:         "Get negative ID if no block present",
			disckblockFn: ShouldGetNegativeIfBlockNotPresent,
		},
		{
			name:         "Initialize New Block",
			disckblockFn: ShouldSuccessfullyInitializeNewBlock,
		},
		{
			name:         "Save Block on Disk",
			disckblockFn: ShouldSaveNewBlockOnDisk,
		},
		{
			name:         "Convert Pairs object To and From Bytes",
			disckblockFn: ShouldConvertPairToAndFromBytes,
		},
		{
			name:         "Convert Disk Block To and From Bytes",
			disckblockFn: ShouldConvertBlockToAndFromBytes,
		},
		{
			name:         "Convert Disk Node To and From Bytes",
			disckblockFn: ShouldConvertToAndFromDiskNode,
		},
	}

	for _, testCase := range testCases {

		s.Run(testCase.name, func() {
			testCase.disckblockFn(s)
		})
	}
}

func TestUnitTestSuite(t *testing.T) {
	suite.Run(t, new(UnitTestSuite))
}
