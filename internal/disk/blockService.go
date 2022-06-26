package disk

import (
	"encoding/binary"
	"os"
	"sync"
)

const blockSize = 4096

const maxLeafSize = 30

func uint64ToBytes(index uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(index))
	return b
}

func uint64FromBytes(b []byte) uint64 {
	return uint64(binary.LittleEndian.Uint64(b))
}

type BlockService struct {
	file      *os.File
	BlockSize int // TODO be sure this can correspond to actual block size
	mu        *sync.Mutex
}

func (bs *BlockService) GetLatestBlockID() (int64, error) {

	fi, err := bs.file.Stat()
	if err != nil {
		return -1, err
	}

	length := fi.Size()
	if length == 0 {
		return -1, nil
	}
	// Calculate page number required to be fetched from disk
	return (int64(fi.Size()) / int64(blockSize)) - 1, nil
}

//@Todo:Store current root block data somewhere else
func (bs *BlockService) GetRootBlock() (*DiskBlock, error) {

	/*
		1. Check if root block exists
		2. If exisits, fetch it, else initialize a new block
	*/
	if !bs.rootBlockExists() {
		// Need to write a new block
		return bs.newBlock()

	}
	return bs.getBlockFromDiskByBlockNumber(0)

}

func (bs *BlockService) getBlockFromDiskByBlockNumber(index int64) (*DiskBlock, error) {
	if index < 0 {
		panic("Index less than 0 asked")
	}
	offset := index * blockSize
	_, err := bs.file.Seek(offset, 0)
	if err != nil {
		return nil, err
	}

	blockBuffer := make([]byte, blockSize)
	_, err = bs.file.Read(blockBuffer)
	if err != nil {
		return nil, err
	}
	block := bs.GetBlockFromBuffer(blockBuffer)
	return block, nil
}

func (bs *BlockService) GetBlockFromBuffer(blockBuffer []byte) *DiskBlock {
	blockOffset := 0
	block := &DiskBlock{}

	//Read Block index
	block.Id = uint64FromBytes(blockBuffer[blockOffset:])
	blockOffset += 8
	block.CurrentLeafSize = uint64FromBytes(blockBuffer[blockOffset:])
	blockOffset += 8
	block.currentChildrenSize = uint64FromBytes(blockBuffer[blockOffset:])
	blockOffset += 8
	//Read actual pairs now
	block.DataSet = make([]*Pairs, block.CurrentLeafSize)
	for i := 0; i < int(block.CurrentLeafSize); i++ {
		block.DataSet[i] = ConvertBytesToPair(blockBuffer[blockOffset:])
		blockOffset += pairSize
	}
	// Read children block indexes
	block.ChildrenBlockIds = make([]uint64, block.currentChildrenSize)
	for i := 0; i < int(block.currentChildrenSize); i++ {
		block.ChildrenBlockIds[i] = uint64FromBytes(blockBuffer[blockOffset:])
		blockOffset += 8
	}
	return block
}

func (bs *BlockService) GetBufferFromBlock(block *DiskBlock) []byte {
	blockBuffer := make([]byte, blockSize)
	blockOffset := 0

	//Write Block index
	copy(blockBuffer[blockOffset:], uint64ToBytes(block.Id))
	blockOffset += 8
	copy(blockBuffer[blockOffset:], uint64ToBytes(block.CurrentLeafSize))
	blockOffset += 8
	copy(blockBuffer[blockOffset:], uint64ToBytes(block.currentChildrenSize))
	blockOffset += 8

	//Write actual pairs now
	for i := 0; i < int(block.CurrentLeafSize); i++ {
		copy(blockBuffer[blockOffset:], ConvertPairsToBytes(block.DataSet[i]))
		blockOffset += pairSize
	}
	// Read children block indexes
	for i := 0; i < int(block.currentChildrenSize); i++ {
		copy(blockBuffer[blockOffset:], uint64ToBytes(block.ChildrenBlockIds[i]))
		blockOffset += 8
	}
	return blockBuffer
}

func (bs *BlockService) newBlock() (*DiskBlock, error) {

	latestBlockID, err := bs.GetLatestBlockID()
	block := &DiskBlock{}
	if err != nil {
		// This means that no file exists
		block.Id = 0
	} else {
		block.Id = uint64(latestBlockID) + 1
	}
	block.CurrentLeafSize = 0
	err = bs.WriteBlockToDisk(block)
	if err != nil {
		return nil, err
	}
	return block, nil
}

func (bs *BlockService) WriteBlockToDisk(block *DiskBlock) error {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	seekOffset := blockSize * block.Id
	blockBuffer := bs.GetBufferFromBlock(block)
	_, err := bs.file.Seek(int64(seekOffset), 0)
	if err != nil {
		return err
	}
	_, err = bs.file.Write(blockBuffer)
	if err != nil {
		return err
	}
	return nil
}

func (bs *BlockService) ConvertDiskNodeToBlock(node *DiskNode) *DiskBlock {
	block := &DiskBlock{Id: node.BlockID}
	tempElements := make([]*Pairs, len(node.GetElements()))
	for index, element := range node.GetElements() {
		tempElements[index] = element
	}
	block.SetData(tempElements)
	tempBlockIDs := make([]uint64, len(node.getChildBlockIDs()))
	for index, childBlockID := range node.getChildBlockIDs() {
		tempBlockIDs[index] = childBlockID
	}
	block.SetChildren(tempBlockIDs)
	return block
}

func (bs *BlockService) getNodeAtBlockID(blockID uint64) (*DiskNode, error) {
	block, err := bs.getBlockFromDiskByBlockNumber(int64(blockID))
	if err != nil {
		return nil, err
	}
	return bs.ConvertBlockToDiskNode(block), nil
}

func (bs *BlockService) ConvertBlockToDiskNode(block *DiskBlock) *DiskNode {
	node := &DiskNode{
		BlockID:      block.Id,
		BlockService: bs,
		Keys:         make([]*Pairs, block.CurrentLeafSize),
	}
	for index := range node.Keys {
		node.Keys[index] = block.DataSet[index]
	}
	node.ChildrenBlockIDs = make([]uint64, block.currentChildrenSize)
	for index := range node.ChildrenBlockIDs {
		node.ChildrenBlockIDs[index] = block.ChildrenBlockIds[index]
	}
	return node
}

// NewBlockFromNode - Save a new node to disk block
func (bs *BlockService) saveNewNodeToDisk(n *DiskNode) error {
	// Get block id to be assigned to this block
	latestBlockID, err := bs.GetLatestBlockID()
	if err != nil {
		return err
	}
	n.BlockID = uint64(latestBlockID) + 1
	block := bs.ConvertDiskNodeToBlock(n)
	return bs.WriteBlockToDisk(block)
}

func (bs *BlockService) updateNodeToDisk(n *DiskNode) error {
	block := bs.ConvertDiskNodeToBlock(n)
	return bs.WriteBlockToDisk(block)
}

func (bs *BlockService) updateRootNode(n *DiskNode) error {
	n.BlockID = 0
	return bs.updateNodeToDisk(n)
}

func NewBlockService(file *os.File) *BlockService {
	vbs := os.Getpagesize()
	return &BlockService{file: file, BlockSize: vbs, mu: &sync.Mutex{}}
}

func (bs *BlockService) rootBlockExists() bool {
	latestBlockID, err := bs.GetLatestBlockID()
	// fmt.Println(latestBlockID)
	//@Todo:Validate the type of error here
	if err != nil {
		// Need to write a new block
		return false
	} else if latestBlockID == -1 {
		return false
	} else {
		return true
	}
}

/**
@Todo: Implement a function to :
1. Dynamicaly calculate blockSize
2. Then based on the blocksize, calculate the maxLeafSize
*/
func (bs *BlockService) GetMaxLeafSize() int {
	// s := bs.VirtualBlockSize
	return maxLeafSize
}
