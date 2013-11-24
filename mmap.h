#ifndef _MMAP_H_
#define _MMAP_H_

#include <string>
#include <unordered_map>
#include <functional>

#include "lrucache.h"

using namespace std;

class ReadonlyMMap {
public:
	ReadonlyMMap();
	// index_fn 索引文件 格式为 key \t offset
	bool init(const string& index_fn, const string& data_fn);
	virtual ~ReadonlyMMap();
	// 查询接口 
	// 查询返回true，v设定成改行
	bool seek(const string& k, string& v);
private:
	// 文件fd
	int _fd;
	// 映射文件指针
	char* _ptr;
	// 文件大小
	off_t _fsize;
	// 索引文件名
	string _index_fn;
	// 数据文件
	string _data_fn;
	unordered_map<size_t, off_t> _index;
	// 缓存
	Cache* _cache;
	// 哈希函数
	std::hash<std::string> _hash;
};

#endif
