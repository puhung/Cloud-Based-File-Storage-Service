package surfstore

import (
	"fmt"
	"log"
	"os"
)

func ClientSync(client RPCClient) {
	fileMetaMap, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		log.Fatal("Failed to LoadMetaFromMetaFile")
	}

	// 1. Check whether file in fileMetaMap has been deleted, updated, unchanged.
	for fileName, fileMetadata := range fileMetaMap {
		if fileName == "index.txt" {
			continue
		}
		filePath, same, blockSize := ConcatPath(client.BaseDir, fileName), true, client.BlockSize

		currentFile, e := os.Open(filePath)
		defer currentFile.Close()
		if e == nil {
			file, err := os.ReadFile(filePath)
			if err != nil {
				fmt.Println("1. Fail to Read file:", fileName)
				log.Fatal(err)
			}

			fileHashes, _ := hashOrBlock(file, blockSize)

			// for i := 0; i < len(file); i += blockSize {
			// 	if i+blockSize < len(file) {
			// 		fileHashes = append(fileHashes, GetBlockHashString(file[i:i+blockSize]))
			// 	} else {
			// 		fileHashes = append(fileHashes, GetBlockHashString(file[i:]))
			// 	}
			// }

			if len(fileHashes) != len(fileMetadata.BlockHashList) {
				same = false
			} else {
				for i := 0; i < len(fileHashes); i++ {
					if fileHashes[i] != fileMetadata.BlockHashList[i] {
						same = false
						fileMetadata.BlockHashList = fileHashes
						break
					}
				}
			}
		} else { // File has been deleted -> Update local fileMetadata
			if fileMetadata.BlockHashList[0] != "0" { // Only update empty file version when the file initially been deleted
				same = false
				fileMetadata.BlockHashList = []string{"0"}
			}
		}
		if !same {
			fileMetadata.Version += 1
		}

	}

	// 2. Check whether there is newly created local file
	entries, err := os.ReadDir(client.BaseDir)
	for _, entry := range entries {
		if entry.IsDir() {
			fmt.Println("Directory:", entry.Name())
		} else {
			localFileName := entry.Name()
			_, exist := fileMetaMap[localFileName]
			if exist || localFileName == "index.txt" {
				continue
			} else {
				blockSize := client.BlockSize

				newLocalFilePath := ConcatPath(client.BaseDir, entry.Name())
				file, err := os.ReadFile(newLocalFilePath)
				if err != nil {
					fmt.Println("2. Fail to open newly create local file:", entry.Name())
					log.Fatal(err)
				}
				fileHashes, _ := hashOrBlock(file, blockSize)
				// fileHashes := []string{}
				// for i := 0; i < len(file); i += blockSize {
				// 	// Put file data into block
				// 	if i+blockSize < len(file) {
				// 		fileHashes = append(fileHashes, GetBlockHashString(file[i:i+blockSize]))
				// 	} else {
				// 		fileHashes = append(fileHashes, GetBlockHashString(file[i:]))
				// 	}
				// }
				// update fileMetaMap
				var fileMetadata = FileMetaData{
					Filename:      localFileName,
					Version:       1,
					BlockHashList: fileHashes}
				fileMetaMap[localFileName] = &fileMetadata
			}
		}
	}
	PrintMetaMap(fileMetaMap)
	// 3. Update BlockStore to put block to BlockStore
	// AND Update MetaStore fileMetaMap using UpdateFile func (serverFileInfoMap)
	var serverFileInfoMap map[string]*FileMetaData
	var blockStoreAddr string
	client.GetBlockStoreAddr(&blockStoreAddr)
	for fileName, fileMetadata := range fileMetaMap {

		var version int32
		client.UpdateFile(fileMetadata, &version)
		client.GetFileInfoMap(&serverFileInfoMap)
		fmt.Println(fileName, "start 3. with version: ", version)
		if version >= 0 { // Local File version == server version + 1 or Local newly created file -> Successfully update MetaStore fileMetaMap -> Update BlockStore Block
			blockSize := client.BlockSize
			// Update block in BlockStore
			if fileMetadata.Version != 1 { // (1) local version == server version + 1
				localHashes := fileMetadata.BlockHashList
				if len(localHashes) == 1 && localHashes[0] == "0" { // If file need to be delete, put empty block in block store
					// Deal with deleted local file
					block := Block{
						BlockData: []byte{},
						BlockSize: 0,
					}
					var succ bool
					client.PutBlock(&block, blockStoreAddr, &succ)
				} else { // The file has been modified
					var serverHashes []string
					client.HasBlocks(localHashes, blockStoreAddr, &serverHashes)
					i_local, i_server := 0, 0

					// Update Modified part in local file
					for i_local < len(localHashes) {
						if i_server < len(serverHashes) && localHashes[i_local] == serverHashes[i_server] {
							i_local += 1
							i_server += 1
						} else {
							var block Block
							var succ bool

							filePath := ConcatPath(client.BaseDir, fileName)
							file, err := os.ReadFile(filePath)
							if err != nil {
								fmt.Println("3. Fail to open file:", fileName)
								log.Fatal(err)
							}
							if i_local+blockSize < len(file) {
								block.BlockData = file[i_local : i_local+blockSize]
								block.BlockSize = int32(blockSize)
							} else {
								block.BlockData = file[i_local:len(file)]
								block.BlockSize = int32(len(file) - i_local)
							}
							client.PutBlock(&block, blockStoreAddr, &succ)
							i_local += 1
						}
					}
				}
			} else { // (2) local newly created file
				var succ bool
				fmt.Println("no logic changed.")
				newLocalFilePath := ConcatPath(client.BaseDir, fileName)
				file, err := os.ReadFile(newLocalFilePath)
				if err != nil {
					fmt.Println("3. Fail to open file:", fileName)
					log.Fatal(err)
				}
				_, blocks := hashOrBlock(file, blockSize)
				for _, block := range blocks {
					client.PutBlock(&block, blockStoreAddr, &succ)
				}
				// for i := 0; i < len(file); i += blockSize {
				// 	// Put file data into block
				// 	if i+blockSize < len(file) {
				// 		block.BlockData = file[i : i+blockSize]
				// 		block.BlockSize = int32(blockSize)
				// 	} else {
				// 		block.BlockData = file[i:]
				// 		block.BlockSize = int32(len(file) - i)
				// 	}
				// 	client.PutBlock(&block, blockStoreAddr, &succ)
				// }
			}
		} else { // Local File version != server version + 1 or Not Local newly created file -> Rewrite local file
			// Server version is higher or Server has same version but different hash or Server does not have the file but the file are not newly created

			// Download the file from serverFileInfoMap
			serverFileMetadata, _ := serverFileInfoMap[fileName]

			localFilePath := ConcatPath(client.BaseDir, fileName)
			_, err := os.Stat(localFilePath)
			if err != nil {
				file, err := os.Create(localFilePath)
				if err != nil {
					fmt.Println("3. Failed to create file:", err)
				}
				defer file.Close()
			}

			targetFile, err := os.OpenFile(localFilePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
			if err != nil {
				fmt.Println("3. Failed to open file:", err)
			}
			defer targetFile.Close()

			serverHashes := serverFileMetadata.BlockHashList
			for _, serverHash := range serverHashes {
				var block Block
				client.GetBlock(serverHash, blockStoreAddr, &block)

				_, err = targetFile.Write(block.BlockData)
				if err != nil {
					fmt.Println("3. Failed to download data from server:", err)
				}
			}

			// Update local fileMetaMap since we modify the local file
			fileMetaMap[fileName] = serverFileInfoMap[fileName]
		}
	}

	//PrintMetaMap(serverFileInfoMap)
	client.GetFileInfoMap(&serverFileInfoMap)
	// 4. Remove need deleted file or Download newly created file from serverFileInfoMap to Local
	// Update the local fileMetaMap
	for serverFileName, serverFileMetadata := range serverFileInfoMap {
		var blockStoreAddr string
		client.GetBlockStoreAddr(&blockStoreAddr)
		if len(serverFileMetadata.BlockHashList) == 1 && serverFileMetadata.BlockHashList[0] == "0" {
			// Remove the file if the file still exist
			if _, err := os.Stat(ConcatPath(client.BaseDir, serverFileName)); os.IsNotExist(err) {
				// fmt.Println("File has been removed:", serverFileName)
				fileMetaMap[serverFileName] = serverFileInfoMap[serverFileName] // always keep fileMetaMap up-to-date as serverFileInfoMap
				continue
			} else {
				err := os.Remove(ConcatPath(client.BaseDir, serverFileName))
				if err != nil {
					fmt.Println(serverFileName)
					fmt.Println("4. Failed to remove file need to be deleted:", err)
				}
				fileMetaMap[serverFileName] = serverFileInfoMap[serverFileName] // always keep fileMetaMap up-to-date as serverFileInfoMap
			}
		} else {
			if _, exist := fileMetaMap[serverFileName]; !exist {
				localFilePath := ConcatPath(client.BaseDir, serverFileName)
				_, err := os.Stat(localFilePath)
				if err != nil {
					file, err := os.Create(localFilePath)
					if err != nil {
						// Handle the error if there was an issue creating the file
						fmt.Println("Error:", err)
						return
					}
					file.Close()
				}

				targetFile, err := os.OpenFile(localFilePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
				if err != nil {
					fmt.Println("Error:", err)
					return
				}
				defer targetFile.Close()

				serverHashes := serverFileMetadata.BlockHashList
				for _, serverHash := range serverHashes {
					var block Block
					client.GetBlock(serverHash, blockStoreAddr, &block)

					_, err = targetFile.Write(block.BlockData)
					if err != nil {
						fmt.Println("4. Failed to download data from server:", err)
					}
				}

				// Update fileMetaMap
				fileMetaMap[serverFileName] = serverFileInfoMap[serverFileName]
			}
		}
	}
	PrintMetaMap(serverFileInfoMap)
	PrintMetaMap(fileMetaMap)
	// 5. Create new fileMetaMap
	WriteMetaFile(fileMetaMap, client.BaseDir)
}

func hashOrBlock(file []byte, blockSize int) ([]string, []Block) {
	fileHashes := []string{}
	blocks := []Block{}
	for i := 0; i < len(file); i += blockSize {
		var block Block

		if i+blockSize < len(file) {
			block.BlockData = file[i : i+blockSize]
			block.BlockSize = int32(blockSize)
			fileHashes = append(fileHashes, GetBlockHashString(file[i:i+blockSize]))
		} else {
			block.BlockData = file[i:]
			block.BlockSize = int32(len(file) - i)
			fileHashes = append(fileHashes, GetBlockHashString(file[i:]))
		}
		blocks = append(blocks, block)
	}
	return fileHashes, blocks
}
