#include "Pool.h"

Pool::Pool()
{
}

Pool::~Pool()
{
}

Pool::Pool(sql::ResultSet * res, map<int, Team>& _teams)
{
	id = res->getInt("pool_id");
	user_id = res->isNull("user_id") ? 0 : res->getInt("user_id");
	name = res->isNull("pool_name") ? "" : res->getString("pool_name");

	int team_id = res->getInt("team_id");
	int max_records = res->getInt("max_records");
	teams.insert(pair<int, int>(team_id, max_records));
	//InsertTeam(team_id, _teams);
}

/***
int Pool::CalculateTotalTeamWeight()
{
	map<int, int>::iterator tit = teams.begin();
	while (tit != teams.end()) {
		team_weight += tit->second;
		tit++;
	}
	return team_weight;
}
***/

/***
void Pool::InsertTeam(int team_id, map<int, Team>& _teams)
{
	map<int, Team>::iterator tit = _teams.find(team_id);
	int team_weight = (tit != _teams.end()) ? tit->second.weight : 1;
	teams.insert(pair<int, int>(team_id, team_weight));
}
***/


