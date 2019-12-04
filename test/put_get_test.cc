#include <cassert> 
#include <string> 
#include <iostream> 
#include "../include/leveldb/db.h" 
#include "../include/leveldb/write_batch.h" 
int main()
{
	leveldb::DB* db;
	leveldb::Options options;
	options.create_if_missing = true;
	//Open         
        leveldb::Status status = leveldb::DB::Open(options,"/tmp/testdb", &db);
	assert(status.ok());

	std::string key1 = "hello";
	std::string value1 = "world!";
	std::string result1;
	//Put 	
        status = db->Put(leveldb::WriteOptions(), key1, value1);
	assert(status.ok());

	//Get 	
        status = db->Get(leveldb::ReadOptions(), key1, &result1);
	std::cout << result1 << std::endl;
	if(status.ok())
	    //Delete 		
              status = db->Delete(leveldb::WriteOptions(), key1);

	std::string key2 = "Test";
	std::string value2 = "Batch!";
	std::string result2;
	//Put 	
        status = db->Put(leveldb::WriteOptions(), key2, value2);
	if(status.ok())
	{
	    //Get 		
                status = db->Get(leveldb::ReadOptions(), key2, &result2);
		std::cout << result2 << std::endl;
		leveldb::WriteBatch bch;
		bch.Delete(key2);
		bch.Put(key2, value2);
		//Batch Write 		
                status = db->Write(leveldb::WriteOptions(), &bch);
		assert(status.ok());
	}
    //Iterator 	
        leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
	for(it->SeekToFirst();it->Valid();it->Next())
		std::cout << it->key().ToString() << ":" << it->value().ToString() << std::endl;
	assert(it->status().ok());
	//Delete Iterator 	
        delete it;

	delete db;
	return 0;

}
