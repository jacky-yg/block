#include <iostream>
#include "db/log_reader.h"
#include "db/version_edit.h"
#include "leveldb/slice.h"
#include "leveldb/env.h"

int main() {
    leveldb::SequentialFile* file;
    //MANIFEST files
    leveldb::Status status = leveldb::Env::Default()->NewSequentialFile("/tmp/leveldbtest-1000/dbbench/MANIFEST-000002", &file);
    std::cout << status.ToString() << std::endl;

    leveldb::log::Reader reader(file, NULL, true/*checksum*/, 0/*initial_offset*/);
    // Read all the records and add to a memtable
    std::string scratch;
    leveldb::Slice record;
    while (reader.ReadRecord(&record, &scratch) && status.ok()) {
        leveldb::VersionEdit edit;
        edit.DecodeFrom(record);
        std::cout << edit.DebugString() << std::endl;
    }
}
