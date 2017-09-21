#include "Team.h"

Team::Team()
{
}


Team::~Team()
{
}

Team::Team(sql::ResultSet * res)
{
	id = res->getInt("id");
	weight = res->getInt("weight");
	team_name = res->getString("team_name");
}

void Team::GeneratePoolStatistics(DbConnectionDll::DbConnection& db)
{
	pools.clear();
	deals.clear();
	callbacks.clear();
	sql::Statement* stmt = db.createStatement();

	//Totals
	stringstream query;
	query << "SELECT pool_id, count(1) AS cnt FROM __workingtable";
	query << " WHERE pool_id > 0";
	query << " AND team_id = ";
	query << id;
	query << " GROUP BY pool_id";
	sql::ResultSet* res = db.executeQuery(stmt, query.str());
	while (res->next()) {
		pools.insert(pair<int, int>(res->getInt("pool_id"), res->getInt("cnt")));
	}
	delete res;

	// Deals
	query.str("");
	query << "SELECT pool_id, count(1) AS cnt FROM __workingtable";
	query << " WHERE pool_id > 0";
	query << " AND isDeal";
	query << " AND team_id = ";
	query << id;
	query << " GROUP BY pool_id";
	res = db.executeQuery(stmt, query.str());
	while (res->next()) {
		deals.insert(pair<int, int>(res->getInt("pool_id"), res->getInt("cnt")));
	}
	delete res;

	// Callbacks
	query.str("");
	query << "SELECT pool_id, count(1) AS cnt FROM __workingtable";
	query << " WHERE pool_id > 0";
	query << " AND !isDeal";
	query << " AND hasCallback";
	query << " AND team_id = ";
	query << id;
	query << " GROUP BY pool_id";
	res = db.executeQuery(stmt, query.str());
	while (res->next()) {
		callbacks.insert(pair<int, int>(res->getInt("pool_id"), res->getInt("cnt")));
	}
	delete res;

	delete stmt;
}
