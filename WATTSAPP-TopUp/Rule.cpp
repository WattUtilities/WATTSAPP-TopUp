#include "Rule.h"



Rule::Rule()
{
}


Rule::~Rule()
{
}

Rule::Rule(sql::ResultSet* res)
{
	id = res->getInt("id");
	old_team_id = res->getInt("old_team_id");
	new_team_id = res->getInt("new_team_id");
	description = res->getString("description");
	query = res->getString("query");
}
