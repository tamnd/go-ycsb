package rocksdb

// RocksDB has to be installed on the host before this builds, and it has
// to be a version the binding matches. The adapter is on
// github.com/linxGnu/grocksdb, which is the maintained fork of the
// tecbot/gorocksdb this used to use and which has had no release since
// 2019. grocksdb tracks RocksDB 10.10.x as of v1.10.8, and RocksDB 11
// dropped C API entry points it still refers to, so a host carrying 11
// fails the build on undefined symbols rather than on anything wrong
// here. Homebrew's rocksdb formula is 11, so on macOS this wants a
// source build of 10.10:
//
//	git clone -b v10.10.1 --depth 1 https://github.com/facebook/rocksdb
//	cd rocksdb && make -j shared_lib install-shared
//
//	CGO_CFLAGS="-I/usr/local/include" \
//	CGO_LDFLAGS="-L/usr/local/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd" \
//	  go build -tags rocksdb ./cmd/go-ycsb
