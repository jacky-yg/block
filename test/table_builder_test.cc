#include <unistd.h>
#include <iostream>
#include <assert.h>
#include "leveldb/env.h"
#include "leveldb/table_builder.h"
#include "leveldb/db.h"
#include "leveldb/dumpfile.h"
#include "leveldb/table.h"
#include <sys/types.h>
#include <sys/stat.h>

void scan_by_table_iterator();

int main() {

    leveldb::DB *db;
    leveldb::Options options;
    options.create_if_missing=true;
    std::string dbpath="testdb";
    leveldb::Status status=leveldb::DB::Open(options,dbpath,&db);
   

    
    options.block_restart_interval = 4;

    std::string file_name("table_builder.data");
    


    leveldb::WritableFile* file;
    status = leveldb::Env::Default()->NewWritableFile(
            file_name,
            &file);

    leveldb::TableBuilder table_builder(options, file);
    table_builder.Add("confuse", "value");
    table_builder.Add("contend", "value");
    table_builder.Add("cope", "value");
    table_builder.Add("copy", "value");
    table_builder.Add("corn", "value");
    
    

   
    status = table_builder.Finish();
    std::cout << status.ToString() << std::endl;

    std::cout << table_builder.NumEntries() << std::endl;
    std::cout << table_builder.FileSize() << std::endl;
    
    
  

    
    file->Close();
    delete db;
   /* uint64_t file_size;
    leveldb:: Status s = leveldb::Env::Default()->GetFileSize(file_name, &file_size);
    std::cout<<file_size<<std::endl;
    leveldb::Table *table = nullptr;
    leveldb::RandomAccessFile* rfile;
    s= leveldb::Env::Default()->NewRandomAccessFile(
            file_name,
            &rfile);
    s = leveldb::Table::Open(options,rfile,file_size,&table);
    
    
    delete table;
    delete file;*/
    scan_by_table_iterator();
    return 0;
}
/*void readblock() {
    leveldb::Table* table = nullptr;
    //std::string file_path = "./data/test_table.db/000005.ldb";
    std::string file_path = "./table_builder.data";

    //New RandomAccessFile
    leveldb::RandomAccessFile* file = nullptr;
    leveldb::Status status = leveldb::Env::Default()->NewRandomAccessFile(
            file_path,
            &file);
    std::cout << "NewRandomAccessFile status:" << status.ToString() << std::endl;

    //New Table
    struct stat file_stat;
    stat(file_path.c_str(), &file_stat);
    status = leveldb::Table::Open(
            leveldb::Options(),
            file,
            file_stat.st_size,
            &table);
    std::cout << "leveldb::Table::Open status:" << status.ToString() << std::endl;
    status = leveldb::Table::InternalReader()

    leveldb::Iterator* iter = table->NewIterator(leveldb::ReadOptions());
    
    iter->Seek("key1");
    std::cout << iter->key().ToString() << "->" << iter->value().ToString() << std::endl;

    table::InternalGet

    while (iter->Valid()) {
        std::cout << iter->key().ToString() << "->" << iter->value().ToString() << std::endl;
        iter->Next();
    }

    delete iter;
    delete file;
    delete table;
}*/

void scan_by_table_iterator() {
    leveldb::Table* table = nullptr;
    //std::string file_path = "./data/test_table.db/000005.ldb";
    std::string file_path = "./table_builder.data";

    //New RandomAccessFile
    leveldb::RandomAccessFile* file = nullptr;
    leveldb::Status status = leveldb::Env::Default()->NewRandomAccessFile(
            file_path,
            &file);
    std::cout << "NewRandomAccessFile status:" << status.ToString() << std::endl;

    //New Table
    struct stat file_stat;
    stat(file_path.c_str(), &file_stat);
    status = leveldb::Table::Open(
            leveldb::Options(),
            file,
            file_stat.st_size,
            &table);

    std::cout << "leveldb::Table::Open status:" << status.ToString() << std::endl;
   //  std::cout<<"before seek"<<std::endl;
    leveldb::Iterator* iter = table->NewIterator(leveldb::ReadOptions());
    
   iter->SeekToFirst();
    if(iter->Valid())
	std::cout<<"valid"<<std::endl;
   iter->Seek("confuse");
   std::cout << iter->key().ToString() << "->" << iter->value().ToString() << std::endl;
   iter->Seek("contend");
   std::cout << iter->key().ToString() << "->" << iter->value().ToString() << std::endl;
   iter->Seek("cope");
   std::cout << iter->key().ToString() << "->" << iter->value().ToString() << std::endl;
   iter->Seek("copy");
   std::cout << iter->key().ToString() << "->" << iter->value().ToString() << std::endl;
   iter->Seek("corn");
   std::cout << iter->key().ToString() << "->" << iter->value().ToString() << std::endl;


   

    /*while (iter->Valid()) {
        std::cout << iter->key().ToString() << "->" << iter->value().ToString() << std::endl;
        iter->Next();
    }*/

    delete iter;
    delete file;
    delete table;
}

