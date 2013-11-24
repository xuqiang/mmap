#include <cstdio>
#include <vector>
#include <functional>
#include <set>
#include <string>
#include <iostream>

using namespace std;

void help() {
	printf("bin infile outfile\n");
}

#define MYCLOSE(p) do { \
	if(p) {				 \
		delete p;		 \
		p = NULL;		\
	}					\
} while(false);

inline void getitemid(const char* l, char* id) {
	id[0] = '\0';
	int i = 0;
	while(l[i]) {
		if(l[i] == '\t' or l[i] == ' ') {
			break;
		}
		id[i] = l[i];
		i++;
	}
	id[i] = '\0';
}

// 输入文件 输出文件
int main(int argc, char* argv[]) {
	if(argc != 3) {
		help();
		return 0;
	}
	const char* infn = argv[1];
	const char* outfn = argv[2];
	FILE* fin = fopen(infn, "r");
	FILE* fout = fopen(outfn, "w");
	const int LINE_SIZE = 1024;
	char l[LINE_SIZE] = { '\0' };
	char ol[LINE_SIZE] = { '\0' };
	char userid[LINE_SIZE] = { '\0' };
	set<size_t> s;
	int coll = 0;

	if(NULL == fin || 
		NULL == fout) {
		printf("open file fail\n");
		goto FAILED;
	}
	
	do {
		long cur = ftell(fin);
		if (fgets(l, LINE_SIZE, fin) == NULL) {
			break;
		}
		getitemid(l, userid);
		
		std::hash<std::string> hash_fn;
		size_t h = hash_fn(userid);
		if(s.find(h) != s.end()) {
			coll++;		
		}
		else {
			s.insert(h);
		}

		snprintf(ol, LINE_SIZE, "%s\t%ld\n", userid, cur);
		fputs(ol, fout);
	} while( 1 );
	
	cout << "coll=" << coll << endl;

	MYCLOSE(fin);
	MYCLOSE(fout);
	return 0;

FAILED:
	MYCLOSE(fin);
	MYCLOSE(fout);
	return 1;
}	
