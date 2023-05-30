package surfstore

import (
	context "context"
	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

var MetaStoreLock sync.Mutex

type MetaStore struct {
	FileMetaMap                  map[string]*FileMetaData
	BlockStoreAddr               string
	UnimplementedMetaStoreServer // this line is the grammar for grpc to claim that this struc implement the MetaStoreserverInterface
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	MetaStoreLock.Lock()
	defer MetaStoreLock.Unlock()
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	fileName := fileMetaData.Filename
	if MetaStoreFileMetaData, exist := m.FileMetaMap[fileName]; exist {
		if fileMetaData.Version-MetaStoreFileMetaData.Version == 1 {
			m.FileMetaMap[fileName] = &FileMetaData{
				Filename:      fileName,
				Version:       fileMetaData.Version,
				BlockHashList: fileMetaData.BlockHashList,
			}
			return &Version{Version: fileMetaData.Version + 1}, nil
		} else if fileMetaData.Version == MetaStoreFileMetaData.Version {
			var same bool
			same = true
			if len(fileMetaData.BlockHashList) != len(MetaStoreFileMetaData.BlockHashList) {
				same = false
			} else {
				for i := 0; i < len(fileMetaData.BlockHashList); i++ {
					if fileMetaData.BlockHashList[i] != MetaStoreFileMetaData.BlockHashList[i] {
						same = false
						break
					}
				}
			}

			if same {
				return &Version{Version: fileMetaData.Version}, nil
			} else {
				return &Version{Version: -1}, nil
			}
		}
	} else {
		if fileMetaData.Version == 1 {
			m.FileMetaMap[fileName] = &FileMetaData{
				Filename:      fileName,
				Version:       fileMetaData.Version,
				BlockHashList: fileMetaData.BlockHashList,
			}
			return &Version{Version: fileMetaData.Version}, nil
		} else {
			return &Version{Version: -1}, nil
		}
	}
	return &Version{Version: -1}, nil
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	MetaStoreLock.Lock()
	defer MetaStoreLock.Unlock()
	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}

func checkHash(fileMetaDataBlockHashList []string, MetaStoreFileMetaDataBlockHashList []string) bool {

}
