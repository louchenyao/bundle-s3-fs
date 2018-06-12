// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Mounts MemNodeFs for testing purposes.

package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"

	"github.com/Chenyao2333/bundle-s3"
	"github.com/Chenyao2333/golang-cache"
)

func filseSize(path string) (int64, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

type bs3File struct {
	nodefs.File
	data []byte
	name string
	b    *bundleS3FS
	bs3  *bundles3.BundleS3
	o    *bundles3.Object
}

// TOOD: lower complexity?

func (f *bs3File) String() string {
	l := len(f.data)
	if l > 10 {
		l = 10
	}
	return fmt.Sprintf("dataFile(%x)", f.data[:l])
}

func (f *bs3File) GetAttr(out *fuse.Attr) fuse.Status {
	out.Mode = fuse.S_IFREG | 0755
	out.Size = uint64(len(f.data))
	fmt.Printf("size: %d", out.Size)
	return fuse.OK
}

func (f *bs3File) Read(buf []byte, off int64) (res fuse.ReadResult, code fuse.Status) {
	fmt.Printf("Read buff_len:%d off:%d data_len:%d\n", len(buf), off, len(f.data))
	end := int(off) + int(len(buf))
	if end > len(f.data) {
		end = len(f.data)
	}
	return fuse.ReadResultData(f.data[off:end]), fuse.OK
}

func (f *bs3File) Write(content []byte, off int64) (uint32, fuse.Status) {
	end := int(off) + int(len(content))
	size := len(f.data)
	if end > size {
		size = end
	}

	tmp := make([]byte, size)
	copy(tmp[:off], f.data[:off])
	copy(tmp[off:end], content)
	if size < len(f.data) {
		copy(tmp[end:size], f.data[end:])
	}

	f.data = tmp
	return uint32(len(tmp)), fuse.OK
}

func (f *bs3File) save() error {
	err := ioutil.WriteFile(f.o.Path(), f.data, 0644)
	if err != nil {
		return err
	}
	err = f.o.Upload(f.bs3)
	f.b.fileSize.Set(f.name, uint64(len(f.data)))
	f.b.cacheTime = 0
	return err
}

func (f *bs3File) Flush() fuse.Status {
	err := f.save()
	if err != nil {
		return fuse.ENOENT
	}
	return fuse.OK
}

func (f *bs3File) Fsync(flags int) (code fuse.Status) {
	err := f.save()
	if err != nil {
		return fuse.ENOENT
	}
	return fuse.OK
}

func (f *bs3File) Truncate(size uint64) (code fuse.Status) {
	tmp := make([]byte, size)
	s := len(f.data)
	if int(size) < s {
		s = int(size)
	}
	copy(tmp[:s], f.data[:s])
	for i := s; i < int(size); i++ {
		tmp[i] = 0
	}
	f.data = tmp
	return fuse.OK
}

func newbs3File(data []byte, name string, b *bundleS3FS, bs3 *bundles3.BundleS3, o *bundles3.Object) nodefs.File {
	f := new(bs3File)
	f.data = data
	f.name = name
	f.b = b
	f.bs3 = bs3
	f.o = o
	f.File = nodefs.NewDefaultFile()
	return f
}

type bundleS3FS struct {
	pathfs.FileSystem
	bs3       *bundles3.BundleS3
	l         []string
	cacheTime int64
	fileSize  *goc.Cache
}

func newbundleS3FS(bs3 *bundles3.BundleS3) *bundleS3FS {
	b := new(bundleS3FS)
	b.FileSystem = pathfs.NewDefaultFileSystem()
	b.bs3 = bs3
	b.cacheTime = 0
	b.fileSize, _ = goc.NewCache("lru", 200)
	return b
}

func (b *bundleS3FS) updateList() {
	if time.Now().Unix()-b.cacheTime > int64(300) {
		l, err := b.bs3.List("")
		if err != nil {
			return
		}
		b.l = l
		b.cacheTime = time.Now().Unix()
	}
}

func isLegalName(name string) bool {
	for c := range []byte(name) {
		if c == '/' {
			return false
		}
	}
	return true
}

func (b *bundleS3FS) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	fmt.Printf("GetAttr %s\n", name)
	// hack this
	if name == "" {
		return &fuse.Attr{
			Mode: fuse.S_IFDIR | 0755,
		}, fuse.OK
	}
	if !isLegalName(name) {
		return nil, fuse.EINVAL
	}
	// in cache
	{
		s, hit := b.fileSize.Get(name)
		if hit == true {
			return &fuse.Attr{
				Mode: fuse.S_IFREG | 0755, Size: s.(uint64),
			}, fuse.OK
		}
	}
	// in remote
	{
		b.updateList()
		for _, n := range b.l {
			if n == name {
				o, err := b.bs3.Get(name)
				if err != nil {
					log.Print(err)
					return nil, fuse.ENOENT
				}
				s, _ := filseSize(o.Path())
				b.fileSize.Set(name, uint64(s))
				return &fuse.Attr{
					Mode: fuse.S_IFREG | 0755, Size: uint64(s),
				}, fuse.OK
			}
		}
	}
	return nil, fuse.ENOENT
}

func (b *bundleS3FS) OpenDir(name string, context *fuse.Context) (c []fuse.DirEntry, code fuse.Status) {
	fmt.Printf("OpenDir %s\n", name)
	if name == "" {
		b.updateList()
		c := make([]fuse.DirEntry, len(b.l))
		for i, n := range b.l {
			c[i] = fuse.DirEntry{Name: n, Mode: fuse.S_IFREG}
		}
		return c, fuse.OK
	}
	return nil, fuse.ENOENT
}

func (b *bundleS3FS) Open(name string, flags uint32, context *fuse.Context) (file nodefs.File, code fuse.Status) {
	if !isLegalName(name) {
		return nil, fuse.ENOENT
	}
	o, err := b.bs3.Get(name)
	if err != nil {
		// TOOD: some other type errors?
		log.Print(err)
		return nil, fuse.ENOENT
	}
	c, err := ioutil.ReadFile(o.Path())
	if err != nil {
		log.Print(err)
		return nil, fuse.ENOENT
	}

	return newbs3File(c, name, b, b.bs3, o), fuse.OK
}

func (b *bundleS3FS) Create(name string, flags uint32, mode uint32, context *fuse.Context) (fuseFile nodefs.File, code fuse.Status) {
	if !isLegalName(name) {
		return nil, fuse.EINVAL
	}
	fmt.Printf("Create %s %d %d\n", name, flags, mode)
	o, err := bundles3.NewObjectFromContent([]byte(""), name)
	if err != nil {
		log.Print(err)
		return nil, fuse.ENOENT
	}

	err = o.Upload(b.bs3)
	if err != nil {
		log.Print(err)
		return nil, fuse.ENOENT
	}
	b.cacheTime = 0
	return newbs3File([]byte(""), name, b, b.bs3, o), fuse.OK
}

func (f *bundleS3FS) Unlink(name string, context *fuse.Context) (code fuse.Status) {
	fmt.Printf("Unlink %s\n", name)
	if !isLegalName(name) {
		return fuse.EINVAL
	}
	f.bs3.Delete(name)
	f.cacheTime = 0
	return fuse.OK
}

func (f *bundleS3FS) StatFs(name string) *fuse.StatfsOut {
	s := syscall.Statfs_t{}
	err := syscall.Statfs("/tmp", &s)
	if err == nil {
		out := &fuse.StatfsOut{}
		out.FromStatfsT(&s)
		return out
	}
	return nil
}

func (f *bundleS3FS) Chmod(path string, mode uint32, context *fuse.Context) (code fuse.Status) {
	return fuse.OK
}

func (f *bundleS3FS) Chown(path string, uid uint32, gid uint32, context *fuse.Context) (code fuse.Status) {
	return fuse.OK
}

func newbs3(ak string, sk string) (*bundles3.BundleS3, error) {
	chunkSize := int64(1024 * 1024) // 1MB

	bs3Cfg, err := bundles3.NewConfig([]bundles3.S3Config{
		bundles3.S3Config{"storage.googleapis.com", ak, sk, "bundles3-0", 0},
		bundles3.S3Config{"storage.googleapis.com", ak, sk, "bundles3-1", 1},
		bundles3.S3Config{"storage.googleapis.com", ak, sk, "bundles3-2", 2},
	}, 2, 1, chunkSize)
	if err != nil {
		return nil, err
	}

	bs3, err := bundles3.NewBundleS3(*bs3Cfg)
	if err != nil {
		return nil, err
	}
	return bs3, nil
}

func main() {
	ak := "GOOGRAQOLAYSQGMH6LT66OJ3"
	sk := "QXYEyoDqBmOhSCVSrs+HYUwSoDz9msola52LMgfX"

	mountPoint := flag.String("m", "/tmp/mountpoint", "The mount point.")

	flag.Parse()

	bs3, err := newbs3(ak, sk)
	if err != nil {
		log.Fatal(err)
	}

	bs3fs := pathfs.NewPathNodeFs(newbundleS3FS(bs3), nil)
	server, _, err := nodefs.MountRoot(*mountPoint, bs3fs.Root(), nil)
	if err != nil {
		log.Fatal(err)
	}
	server.Serve()
	log.Printf("Serving...")
}
