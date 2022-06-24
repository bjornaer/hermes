package disk_test

import (
	"os"
	"testing"

	"github.com/bjornaer/hermes/internal/disk"
)

func initBlockService() *disk.BlockService {
	path := "./db/test.db"
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
	return disk.NewBlockService(file)
}

func TestShouldGetNegativeIfBlockNotPresent(t *testing.T) {
	blockService := initBlockService()
	latestBlockID, _ := blockService.GetLatestBlockID()
	if latestBlockID != -1 {
		t.Error("Should get negative block id")
	}
}

func TestShouldSuccessfullyInitializeNewBlock(t *testing.T) {
	blockService := initBlockService()
	block, err := blockService.GetRootBlock()
	if err != nil {
		t.Error(err)
	}
	if block.Id != 0 {
		t.Error("Root Block id should be zero")
	}

	if block.CurrentLeafSize != 0 {
		t.Error("Block leaf size should be zero")
	}
}

func TestShouldSaveNewBlockOnDisk(t *testing.T) {
	blockService := initBlockService()
	block, err := blockService.GetRootBlock()
	if err != nil {
		t.Error(err)
	}
	if block.Id != 0 {
		t.Error("Root Block id should be zero")
	}

	if block.CurrentLeafSize != 0 {
		t.Error("Block leaf size should be zero")
	}
	elements := make([]*disk.Pairs, 3)
	elements[0] = disk.NewPair("hola", "amigos")
	elements[1] = disk.NewPair("foo", "bar")
	elements[2] = disk.NewPair("gooz", "bumps")
	block.SetData(elements)
	err = blockService.WriteBlockToDisk(block)
	if err != nil {
		t.Error(err)
	}

	block, err = blockService.GetRootBlock()
	if err != nil {
		t.Error(err)
	}

	if len(block.DataSet) == 0 {
		t.Error("Length of data field should not be zero")
	}
}

func TestShouldConvertPairToAndFromBytes(t *testing.T) {
	pair := &disk.Pairs{}
	pair.SetKey("Hola  ")
	pair.SetValue("Amigos")
	pairBytes := disk.ConvertPairsToBytes(pair)
	convertedPair := disk.ConvertBytesToPair(pairBytes)

	if pair.KeyLen != convertedPair.KeyLen || pair.ValueLen != convertedPair.ValueLen {
		t.Error("Lengths do not match")
	}

	if pair.Key != convertedPair.Key || pair.Value != convertedPair.Value {
		t.Error("Values do not match")
	}
}

func TestShouldConvertBlockToAndFromBytes(t *testing.T) {
	blockService := initBlockService()
	block := &disk.DiskBlock{}
	block.SetChildren([]uint64{2, 3, 4, 6})

	elements := make([]*disk.Pairs, 3)
	elements[0] = disk.NewPair("hola", "amigos")
	elements[1] = disk.NewPair("foo", "bar")
	elements[2] = disk.NewPair("gooz", "bumps")
	block.SetData(elements)
	blockBuffer := blockService.GetBufferFromBlock(block)
	convertedBlock := blockService.GetBlockFromBuffer(blockBuffer)

	if convertedBlock.ChildrenBlockIds[2] != 4 {
		t.Error("Should contain 4 at 2nd index")
	}

	if len(convertedBlock.DataSet) != len(block.DataSet) {
		t.Error("Length of blocks should be same")
	}

	if convertedBlock.DataSet[1].Key != block.DataSet[1].Key {
		t.Error("Keys dont match")
	}

	if convertedBlock.DataSet[2].Value != block.DataSet[2].Value {
		t.Error("Values dont match")
	}
}

func TestShouldConvertToAndFromDiskNode(t *testing.T) {
	bs := initBlockService()
	node := &disk.DiskNode{}
	node.BlockID = 55
	elements := make([]*disk.Pairs, 3)
	elements[0] = disk.NewPair("hola", "amigos")
	node.Keys = elements
	node.ChildrenBlockIDs = []uint64{1000, 10001}
	block := bs.ConvertDiskNodeToBlock(node)

	if block.Id != 55 {
		t.Error("Should have same block id as node block id")
	}
	if block.ChildrenBlockIds[1] != 10001 {
		t.Error("Block ids should match")
	}

	nodeFromBlock := bs.ConvertBlockToDiskNode(block)

	if nodeFromBlock.BlockID != node.BlockID {
		t.Error("Block ids should match")
	}

	if nodeFromBlock.ChildrenBlockIDs[0] != 1000 {
		t.Error("Child Block ids should match")
	}
	if nodeFromBlock.Keys[0].Key != "hola" {
		t.Error("Data elements should match")
	}
}
