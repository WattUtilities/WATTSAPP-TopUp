#pragma once

#include <string>
#include <map>
#include "C:/Users/Rob/Documents/Visual Studio 2015/Projects/DbConnection/DbConnection/DbConnection.h"

using namespace std;

class Team
{
public:
	Team();
	~Team();
	Team(sql::ResultSet* res);
	void GeneratePoolStatistics(DbConnectionDll::DbConnection& db);

	int id;
	int weight;
	string team_name;
	map<int, int> pools;
	map<int, int> deals;
	map<int, int> callbacks;
};

