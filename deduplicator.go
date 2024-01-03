package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/misakacoder/persistence"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type File struct {
	path string
	size int64
}

var (
	filename               = "deduplicator.dt"
	pst                    = persistence.GOB(filename)
	duplicateFileDirectory = "duplicate_files"
	cache                  = map[string]map[int64][]string{}
	goroutines             = 256
	ch                     = make(chan File, goroutines)
	lock                   = sync.Mutex{}
	wg                     = sync.WaitGroup{}
)

func main() {
	var directory string
	fmt.Print("please input directory: ")
	fmt.Scan(&directory)
	if !DirectoryExist(directory) {
		log.Fatalf("directory '%s' does not exist\n", directory)
	}
	pst.Decode(&cache)
	for i := 0; i < goroutines; i++ {
		go CalculateFileHashAndCache()
		wg.Add(1)
	}
	filepath.WalkDir(directory, func(path string, entry fs.DirEntry, err error) error {
		if !entry.IsDir() {
			info, _ := entry.Info()
			size := info.Size()
			ch <- File{path: path, size: size}
		}
		return err
	})
	close(ch)
	wg.Wait()
	for hash, v := range cache {
		for size, paths := range v {
			if len(paths) > 1 {
				directory = CreateDuplicateFileDirectory(hash)
				for _, path := range paths {
					if path != "exist" {
						log.Printf("duplicate: %s\n", path)
						err := os.Rename(path, filepath.Join(directory, filepath.Base(path)))
						if err != nil {
							log.Println(err)
						}
					}
				}
			}
			v[size] = []string{"exist"}
		}
	}
	pst.Encode(cache)
}

func CreateDuplicateFileDirectory(directory string) string {
	directory = filepath.Join(duplicateFileDirectory, directory)
	err := os.MkdirAll(directory, 0777)
	if err != nil {
		log.Fatalln(err)
	}
	return directory
}

func DirectoryExist(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

func CalculateFileHashAndCache() {
	for {
		file, ok := <-ch
		if !ok {
			break
		}
		path := file.path
		size := file.size
		hash := CalculateFileHash(path)
		exist := false
		lock.Lock()
		if v, ok := cache[hash]; ok {
			if paths, ok := v[size]; ok {
				v[size] = append(paths, path)
				exist = true
			}
		}
		if !exist {
			cache[hash] = map[int64][]string{
				size: {path},
			}
		}
		lock.Unlock()
		time.Sleep(time.Millisecond)
	}
	wg.Done()
}

func CalculateFileHash(filename string) string {
	f, err := os.OpenFile(filename, os.O_RDONLY, 0777)
	defer f.Close()
	if err != nil {
		log.Fatalln(err)
	}
	hash := sha256.New()
	if _, err = io.Copy(hash, f); err != nil {
		log.Fatalln(err)
	}
	return hex.EncodeToString(hash.Sum(nil))
}
