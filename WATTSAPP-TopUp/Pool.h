#pragma once

#include <map>
#include "C:/Users/Rob/Documents/Visual Studio 2015/Projects/DbConnection/DbConnection/DbConnection.h"
#include "Team.h"

using namespace std;

class Pool
{
public:
	Pool();
	Pool(sql::ResultSet * res, map<int, Team>& _teams);
	~Pool();
	//int CalculateTotalTeamWeight();
	//void InsertTeam(int rule_id, map<int, Team>& _teams);

	int id;
	int user_id;
	string name;
	int team_weight;
	map<int, int> teams;
};

