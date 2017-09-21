#pragma once

#include <string>
#include "C:/Users/Rob/Documents/Visual Studio 2015/Projects/DbConnection/DbConnection/DbConnection.h"

using namespace std;

class Rule
{
public:
	Rule();
	~Rule();
	Rule(sql::ResultSet* res);

	int id;
	int old_team_id;
	int new_team_id;
	string description;
	string query;
};

