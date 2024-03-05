// General Libraries
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include "csv.hpp"



// RocksDB Libraries
#include <rocksdb/db.h>
#include <rocksdb/options.h>

// Namespaces
using namespace std;
using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::DBOptions;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Slice;

// Function to create a RocksDB instance and load data from CSV file
DB* create_kvs(const string& csv_file_path, const string& db_path) {

    // Open the CSV file
    csv::CSVReader reader(csv_file_path);
    csv::CSVRow row;


    vector<string> header = reader.get_col_names();

    Options options;
    options.create_if_missing = true;


    DB* db;
    Status dbstatus = DB::Open(options, db_path, &db);
    if (!dbstatus.ok()) {
        cerr << "Can't open rocksDB " << dbstatus.ToString() << endl;
        return nullptr;
    }

    // Write data to RocksDB
    WriteBatch batch;
    WriteOptions write_options;

    int col_no = 0;
    for (csv::CSVRow& row : reader) {
        string id_value = row["id"].get<string>();
        col_no = 0;
        //cout << "ID: " << row["col_name"] << endl;
        for (int i = 0; i < header.size(); i++) {
            string key = id_value + "_" + header[i];
            string value = row[i].get<string>();
            //cout << header[col_no]  << ": " << row[i].get<string>() << endl;
            col_no++;
            batch.Put(key, value);
        }
    }


    dbstatus = db->Write(write_options, &batch);
    if (!dbstatus.ok()) {
        cerr << "Error: " << dbstatus.ToString() << endl;
        delete db;
        return nullptr;
    }

    return db;
}

// Function to perform a MultiGet operation
vector<string> multi_get(DB* db, const vector<string>& keys) {

    ReadOptions options;
    vector<Slice> slices;
    vector<string> values;

    for (const auto& key : keys) {
        slices.push_back(key);
    }

    vector<Status> statuses = db->MultiGet(options, slices, &values);

    for (Status& status : statuses) {
        if (!status.ok()) {
            cout << "Error: " << status.ToString() << endl;
        }
    }
    return values;
}



vector<string> iterate_over_range(DB* db, const string& start_key, const string& end_key) {
    vector<string> result;

    ReadOptions readOptions;

    rocksdb::Iterator* it = db->NewIterator(readOptions);

    string start_key_display_name = start_key + "_display_name";
    string end_key_display_name = end_key + "_display_name";

    it->Seek(start_key_display_name);


    while (it->Valid() && it->key().ToString() < end_key_display_name) {
        auto key = it->key().ToString();
        auto value = it->value().ToString();
        auto pos = key.find("display_name");
        if (pos != string::npos) {
            auto start = value.find(':') + 1;
            auto end = value.find('"', start);
            auto display_name = value.substr(start, end - start);
            result.push_back(display_name);
        }
        it->Next();
    }

    delete it;

    return result;
}



// Delete_key
Status delete_key(DB* db, const string& key) {
    Status s;
    WriteBatch wb;
    wb.Delete(key);
    Status status = db->Write(WriteOptions(), &wb);

    if (!status.ok()) {
        cerr << "Error : " << status.ToString() << endl;
    }
    return s;
}
