package surfstore

import (
	context "context"
	"errors"
	"sync"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

var BlockStoreLock sync.Mutex

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	BlockStoreLock.Lock()
	defer BlockStoreLock.Unlock()

	block, exists := bs.BlockMap[blockHash.Hash] // block is the block we found, exists is the boolean value
	if exists {
		return block, nil
	} else {
		return nil, errors.New("Failed to get Block")
	}

}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	BlockStoreLock.Lock()
	defer BlockStoreLock.Unlock()
	blockHash := GetBlockHashString(block.BlockData)
	bs.BlockMap[blockHash] = block
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	BlockStoreLock.Lock()
	defer BlockStoreLock.Unlock()

	blockHashes := &BlockHashes{Hashes: []string{}}
	for _, blockHash := range blockHashesIn.Hashes {
		_, exists := bs.BlockMap[blockHash]
		if exists {
			blockHashes.Hashes = append(blockHashes.Hashes, blockHash)
		}
	}
	return blockHashes, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
