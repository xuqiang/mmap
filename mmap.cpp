#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <cstring>
#include "glog/logging.h"
#include "baselib/text.h"

#include "mmap.h"

using namespace std;

static void deleter(const Slice& key, void* value) {
	if(value) {
		free(value);
		value = NULL;
	}
}


ReadonlyMMap::ReadonlyMMap() : _fd(-1), _ptr(NULL), _fsize(0), _cache(NULL) { }

bool ReadonlyMMap::init(const string& index_fn, const string& data_fn) {
	_index_fn = index_fn;
	_data_fn = data_fn;
	
	_fd = open(_data_fn.c_str(), O_RDWR);
	if(_fd == -1) {
		LOG(INFO) << "open file " << _data_fn << " fail";
		return false;	
	}
	
	struct stat sb;
	fstat(_fd, &sb);
	_fsize = sb.st_size;
	_ptr = (char* )mmap(NULL, sb.st_size, PROT_READ, MAP_SHARED, _fd, 0);
	if (_ptr == MAP_FAILED) { 
		LOG(INFO) << "mmap data file fail";
		return false;
	}

	LOG(INFO) << "begin to load index file";
	FILE* fin = fopen(_index_fn.c_str(), "r");
	if(fin == NULL) {
		LOG(INFO) << "open index file " << _index_fn << " fail";
		return false;
	}
	const int LINE_SIZE = 1024;
	char line[LINE_SIZE];
	while(fgets(line, LINE_SIZE, fin) != NULL) {
		// LOG(INFO) << "read line " << line;
		std::vector<std::string> i = StringSplitter::split(line, "\t");
		if(i.size() != 2) {
			LOG(INFO) << " read bad line " << line;
			continue;
		}
		size_t h = _hash(i[0]);
		off_t offset = atol(i[1].c_str());
		
		_index[h] = offset;
		// LOG(INFO) << "init_mmap hash value " << h << " offset " << offset;
	}
	
	fclose(fin);
	
	// 加载cache
	const int cache_size = 1000000;
	_cache = NewLRUCache(cache_size);
	if(_cache == NULL) {
		LOG(INFO) << "new lrucache fail";
		return false;
	}

	return true;
}	

ReadonlyMMap::~ReadonlyMMap() {
	if(_ptr) 
		munmap(_ptr, _fsize);
	if(_fd != -1) 
		close(_fd);
}


bool ReadonlyMMap::seek(const string& k, string& v) {
	// 命中cache
	Cache::Handle* handle = _cache->Lookup(k);
	if(handle != NULL) {
		LOG(INFO) << "hit cache for key " << k;
		v.clear();
		v = (char*)_cache->Value(handle);
		_cache->Release(handle);
		
		return true;
	}

	size_t h = _hash(k);
	if(_index.find(h) == _index.end()) {
		return false;
	}
	
	off_t offset = _index[h];
	LOG(INFO) << "userid " << k  << " hash " << h << " offset " << offset << endl;
	off_t s = offset;
	while(true) {
		if(offset <= _fsize && _ptr[offset] != '\n') {
			offset++;
		}
		else {
			break;
		}
	}
	v.clear();
	v.append(&_ptr[s], offset - s + 1);
	LOG(INFO) << "seek key " << k << " value " << v << " init offset " << s << " after offset " << offset; 

	// 将数据添加到cache中
	void* value = malloc(sizeof(char) * v.size());
	if(value != NULL) {
		memcpy(value, v.c_str(), v.size());
		Cache::Handle* handle = _cache->Insert(k.c_str(), value, 1, deleter);
		_cache->Release(handle);
		LOG(INFO) << "insert cache for k "  << k;
	}
	else {
		LOG(INFO) << "malloc cache memory fail";
	}

	return true;
}


