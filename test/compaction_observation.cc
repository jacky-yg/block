/*
 * compaction_observation.cc
 *
 *  Created on: Dec 16, 2019
 *      Author: jacky
 */

#include "leveldb/db.h"
#include "leveldb/env.h"
#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <iostream>
#include <sstream>



using namespace std;

string ZeroPadNumber(int num);

int main()
{
    leveldb::DB* db = NULL;
    leveldb::Options options;
    options.create_if_missing = true;

    std::string dbpath="testdb";
    leveldb::Status status=leveldb::DB::Open(options,dbpath,&db);
    assert(status.ok());

	srand(5);
	int entries = 10000000;
	int min = 1;
	int max = 100000000;
	std::string value = "1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111";

	for(int i = 0; i < entries; i++)
	{

		int num = rand() % (max - min + 1) + min;


        string zero_num = ZeroPadNumber(num);
	    string key = "test_key_number_";
	    key.append(zero_num);

	    //std::cout<<"key:"<<key<<",value:"<<value<<std::endl;
		status = db->Put(leveldb::WriteOptions(),key,value);
		assert(status.ok());
	}



    delete db;
    return 0;

}


string ZeroPadNumber(int num)
{
	stringstream ss;

	// the number is converted to string with the help of stringstream
	ss << num;
	string ret;
	ss >> ret;

	// Append zero chars
	int str_length = ret.length();
	for (int i = 0; i < 10 - str_length; i++)
		ret = "0" + ret;
	return ret;
}

