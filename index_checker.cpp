#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <unordered_map>

using namespace std;

int main(int argc, char* argv[]) {
	if(argc != 3) {
		printf("index_checker old_index new_index\n");
		exit(0);
	}
	const char* oldindex_fn = argv[1];
	const char* newindex_fn = argv[2];
	FILE* oldfin = fopen(oldindex_fn, "r");
	FILE* newfin = fopen(newindex_fn, "r");
	if(oldfin == NULL || newfin == NULL) {
		printf("open file:%s or %s error\n", oldindex_fn, newindex_fn);
		exit(1);
	}
	char line[10240] = { '\0' };
	unordered_map<long, long> oldindex;
	long item_id;
	long off;
	long loads = 0;
	while( fgets(line, 10240, oldfin) ) {
		sscanf(line, "%ld\t%ld", &item_id, &off);
		if(oldindex.find(item_id) != oldindex.end()) {
			printf("dup item_id:%ld index:%ld index2:%ld \n", oldindex[item_id], off);
		}
		oldindex[ item_id ] = off;
		if( loads++ % 100000 == 0 ) {
			printf("load %s total %ld\n", oldindex_fn, loads);
		}
	}

	loads = 0;
	long error = 0;
	while( fgets( line, 10240, newfin ) ) {
		sscanf( line, "%ld\t%ld", &item_id, &off );
		if( oldindex.find( item_id ) == oldindex.end() ) {
			printf( "item_id:%ld not found\n", item_id );
			if(++error >= 100) exit(1);
		} else if( oldindex[item_id] != off ) {
			printf("item_id:%ld oldoff:%ld newoff:%ld\n", item_id, oldindex[item_id], off);
			if(++error >= 100) exit(1);
		}
		if( loads++ % 100000 == 0 ) {
			printf("load %s total %ld\n", newindex_fn, loads);
		}
	}
	printf("index check ok!\n");
	exit(1);
}
