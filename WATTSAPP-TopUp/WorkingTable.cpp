#include "WorkingTable.h"

WorkingTable::WorkingTable()
{
}


WorkingTable::~WorkingTable()
{
}

void WorkingTable::CreateTable(DbConnectionDll::DbConnection & db)
{
	cout << "Create Working Table ..." << endl;
	stringstream query;
	query << "DROP TABLE IF EXISTS __workingtable";
	sql::Statement* stmt = db.createStatement();
	db.execute(stmt, query.str());

	query.str("");
	query << "CREATE TABLE IF NOT EXISTS __workingtable(";
	query << " site_id int unsigned not null default 0,";
	query << " grouping_id int unsigned not null default 0,";
	query << " team_id int unsigned not null default 0,";
	query << " pool_id int unsigned not null default 0,";
	query << " supplier int unsigned not null default 0,";
	query << " source int unsigned not null default 0,";
	query << " eac bigint default 0,";
	query << " aq bigint default 0,";
	query << " days_left int default 0,";
	query << " hasCallback int unsigned default 0,";
	query << " isDeal bool not null default false,";
	query << " isCeased bool not null default false,";
	query << " isNightData bool not null default false,";
	query << " isHalfHourly bool not null default false,";
	query << " isMaxDemand bool not null default false,";
	query << " isRelated bool not null default false,";
	query << " isConfirmedByLastRefresh bool not null default false,";
	query << " isConfirmedByPreviousRefresh bool not null default false,";
	query << " isFlexible bool not null default false,";
	query << " isVariable bool not null default false,";
	query << " isDefault bool not null default false,";
	query << " isDeemed bool not null default false,";
	query << " isTariff bool not null default false,";
	query << " isDeEnergised bool not null default false,";
	query << " numNotes int unsigned not null default 0,";
	query << " primary key(site_id, grouping_id)";
	query << ")";
	stmt = db.createStatement();
	db.execute(stmt, query.str());

	delete stmt;
}

void WorkingTable::Populate(DbConnectionDll::DbConnection & db)
{
	sql::Statement* stmt = db.createStatement();
	stringstream query;

	// Add single sites
	cout << "Adding Single Sites ..." << endl;
	query << "INSERT INTO __workingtable";
	query << " SELECT";
	query << " s.id,";	// site_id
	query << " 0,";		// groupinbg_id
	query << " 0,";		// team_id
	query << " s.pool_id,";	// pool_id
	query << " 0,";		// supplier
	query << " s.source,";		// source
	query << " 0,";	// eac
	query << " 0,";	// aq
	query << " 0,";	// days_left
	query << " FALSE,"; // has callback
	//query << " cb.id,"; // has callback
	query << " FALSE,";	// is deal
	query << " s.ceased,"; // is ceased
	query << " s.night_data,"; // is night data
	query << " FALSE,"; // is half hourly
	query << " FALSE,";	// is max demand
	query << " FALSE,";	// is related meter
	query << " FALSE,";	// is confirmed by last refresh
	query << " FALSE,";	// is confirmed by previous refresh
	query << " FALSE,";	// is flexible
	query << " FALSE,";	// is variable
	query << " FALSE,";	// is default
	query << " FALSE,";	// is deemed
	query << " FALSE,";	// is tariff
	query << " FALSE,";	// is de-energised
	query << " 0";	// number of notes
	//query << " COUNT(DISTINCT n.id)";	// number of notes
	query << " FROM sites s";
	query << " LEFT JOIN meters m ON m.site_id = s.id";
	query << " LEFT JOIN contracts c ON m.contract_id = c.id";
	//query << " LEFT JOIN callbacks cb ON cb.site_id = s.id";
	//query << " LEFT JOIN notes n ON n.site_id = s.id";
	query << " WHERE s.grouping_id = 0";
	query << " GROUP BY s.id";
	db.execute(stmt, query.str());

	// Add site groups
	// Note: use MAX(cb_id) in case only some sites have callbacks
	cout << "Adding Site Groups ..." << endl;
	query.str("");
	query << "INSERT INTO __workingtable";
	query << " SELECT";
	query << " 0,";	// site_id
	query << " s.grouping_id,";		// grouping_id
	query << " 0,";		// team_id
	query << " s.pool_id,";	// pool_id
	query << " 0,";		// supplier
	query << " s.source,";		// source
	query << " 0,";	// eac
	query << " 0,";	// aq
	query << " 0,";	// days_left
	query << " FALSE,"; // has callback
	//query << " MAX(cb.id),"; // has callback
	query << " FALSE,";	// is deal
	query << " MIN(s.ceased),"; // is ceased
	query << " MIN(s.night_data),"; // is night data
	query << " FALSE,"; // is half hourly
	query << " FALSE,";	// is max demand
	query << " FALSE,";	// is related meter
	query << " FALSE,";	// is confirmed by last refresh
	query << " FALSE,";	// is confirmed by previous refresh
	query << " FALSE,";	// is flexible
	query << " FALSE,";	// is variable
	query << " FALSE,";	// is default
	query << " FALSE,";	// is deemed
	query << " FALSE,";	// is tariff
	query << " FALSE,";	// is de-energised
	query << " 0";	// number of notes
	//query << " COUNT(DISTINCT n.id)";	// number of notes
	query << " FROM sites s";
	query << " LEFT JOIN meters m ON m.site_id = s.id";
	query << " LEFT JOIN contracts c ON m.contract_id = c.id";
	//query << " LEFT JOIN callbacks cb ON cb.site_id = s.id";
	//query << " LEFT JOIN notes n ON n.site_id = s.id";
	query << " WHERE s.grouping_id > 0";
	query << " GROUP BY s.grouping_id";
	db.execute(stmt, query.str());
	delete stmt;

	SetDaysLeft(db);
	SetCallbackFlag(db);
	FindDealStatus(db);

	string refresh_date = "2017-04-01";
	FlagRefreshConfirmedMeters(db, "isConfirmedByLastRefresh", refresh_date);
	refresh_date = "2017-01-16";
	FlagRefreshConfirmedMeters(db, "isConfirmedByPreviousRefresh", refresh_date);
	FindSupplier(db);
	
	FlagHalfHourlies(db);
	FlagMaxDemands(db);
	FlagRelatedMeters(db);
	CalculateConsumption(db);

	FlagFlexibleTypeContracts(db, "Flexible");
	FlagFlexibleTypeContracts(db, "Variable");
	FlagFlexibleTypeContracts(db, "Default");
	FlagFlexibleTypeContracts(db, "Deemed");
	FlagFlexibleTypeContracts(db, "Tariff");
	FlagDeEnergisedMeters(db);
	//FindSource(db); Not necessary because source is set on extract from sites
	CountNotes(db);
}

void WorkingTable::BuildIndexes(DbConnectionDll::DbConnection& db)
{
	BuildIndex("team_id", "__workingtable", db);
	BuildIndex("pool_id", "__workingtable", db);
	BuildIndex("eac", "__workingtable", db);
	BuildIndex("aq", "__workingtable", db);
	BuildIndex("days_left", "__workingtable", db);
	BuildIndex("isDeal", "__workingtable", db);
	BuildIndex("isCeased", "__workingtable", db);
	BuildIndex("isNightData", "__workingtable", db);
	BuildIndex("source", "__workingtable", db);
	BuildIndex("numNotes", "__workingtable", db);
}

void WorkingTable::BuildIndex(string idx_name, string tbl_name, DbConnectionDll::DbConnection& db)
{
	sql::Statement* stmt = db.createStatement();

	stringstream query;
	query << "ALTER TABLE " << tbl_name << " DROP INDEX " << idx_name;
	try {
		db.execute(stmt, query.str());
		cout << "Dropping index on " << idx_name;
	}
	catch (sql::SQLException& e) {
		cout << "No index on " << idx_name;
	}
	query.str("");
	cout << " : Adding index on " << idx_name << endl;
	query << "ALTER TABLE " << tbl_name << " ADD INDEX " << idx_name << "(" << idx_name << ")";
	db.execute(stmt, query.str());

	delete stmt;
}

void WorkingTable::DropTable(DbConnectionDll::DbConnection & db)
{
	cout << "Drop Working Table" << endl;
	stringstream query;
	query << "DROP TABLE __workingtable";
	sql::Statement* stmt = db.createStatement();
	db.execute(stmt, query.str());

	delete stmt;
}

void WorkingTable::Segment(map<int, Rule>& rules, DbConnectionDll::DbConnection & db, bool bCorp)
{
	cout << "Segmenting ..." << endl;
	sql::Statement* stmt = db.createStatement();
	map<int, Rule>::iterator rit;
	for (rit = rules.begin(); rit != rules.end(); rit++) {
		if (bCorp) { // if corporate only process the following teams
			switch (rit->second.new_team_id) {
			case  9: // < 5000 eac
			case 11: // ceased
			case 12: // night data
			case 16: // de-energised
				break;
			default:
				continue;
				break;
			}
		}
		cout << "\tRule: " << rit->second.description << endl;
		stringstream query;
		query << "UPDATE __workingtable";
		query << " SET team_id = " << rit->second.new_team_id;
		query << " WHERE team_id = " << rit->second.old_team_id;
		query << " AND (" << rit->second.query << ")";
		db.executeUpdate(stmt, query.str());
	}
	delete stmt;
}

void WorkingTable::FreeNonMatchingSites(map<int, Team>& teams, DbConnectionDll::DbConnection& db)
{
	cout << "Freeing non-matching sites ..." << endl;
	sql::Statement* stmt = db.createStatement();
	
	Team unassigned;
	unassigned.id = 0;
	unassigned.weight = 0;
	unassigned.team_name = "Unassigned";

	map<int, Team> teamsPlus = teams;
	teamsPlus.insert(pair<int, Team>(0, unassigned));
	map<int, Team>::iterator tit = teamsPlus.begin();
	while (tit != teamsPlus.end()) {
		cout << "\tTeam: " << tit->second.team_name << endl;
		stringstream query;
		query << "UPDATE __workingtable";
		query << " SET pool_id = 0";
		query << " WHERE !isDeal";
		query << " AND team_id = " << tit->first;
		query << " AND pool_id NOT IN (";
		query << " SELECT pool_id";
		query << " FROM pool_team";
		query << " WHERE team_id = ";
		query << tit->first;
		query << " UNION";
		query << " SELECT id";
		query << " FROM pools";
		query << " WHERE lock_sites";
		query << " )";
		// This section added by RKM on 18/04/2017
		query << " AND (";
		query << " pool_id NOT IN (";
		query << " SELECT u.primary_pool_id";
		query << " FROM pool_team pt";
		query << " LEFT JOIN pool_user pu ON pu.pool_id = pt.pool_id";
		query << " LEFT JOIN users u ON u.id = pu.user_id";
		query << " WHERE pt.team_id = ";
		query << tit->first;
		query << " ) OR !hasCallback)";
		// End section **/
		db.executeUpdate(stmt, query.str());
		tit++;

		/**
		map<int, int>::iterator pit = rit->second.pools.begin();
		while (pit != rit->second.pools.end()) {
			if (pit != rit->second.pools.begin())
				query << ",";
			query << pit->first;
			pit++;
		}
		**/
	}
	delete stmt;
}

/****
void WorkingTable::FreeNonMatchingSites(map<int, Pool>& pools, DbConnectionDll::DbConnection& db)
{
	cout << "Freeing non-matching sites ..." << endl;
	stringstream query;
	query << "SELECT * FROM __workingtable WHERE isDeal = 0";
	sql::Statement* stmt = db.createStatement();
	sql::ResultSet* res = db.executeQuery(stmt, query.str());
	int itCnt = 0;
	while (res->next())
	{
		int site = res->getInt("site_id");
		int group = res->getInt("grouping_id");
		int rule = res->getInt("rule");
		int pool = res->getInt("pool_id");
		map<int, Pool>::iterator pit = pools.find(pool);
		if (pit != pools.end())
		{
			map<int, int>::iterator rit = pit->second.rules.find(rule);
			if (rit == pit->second.rules.end()) //pool not eligible for this rule
			{
				if(site > 0)
					FreeNonMatchingSite(site, db);
				if(group > 0)
					FreeNonMatchingGroup(group, db);
			}
		}
		if ((++itCnt % 1000) == 0)
			cout << ".";
	}
	delete res;
	delete stmt;
	cout << endl;
}
****/

void WorkingTable::FreeNonMatchingSite(int site_id, DbConnectionDll::DbConnection& db)
{
	stringstream query;
	query << "UPDATE sites SET pool_id = 0 WHERE id = " << site_id;
	sql::Statement* stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());
	delete stmt;
}

void WorkingTable::FreeNonMatchingGroup(int group_id, DbConnectionDll::DbConnection& db)
{
	stringstream query;
	query << "UPDATE sites SET pool_id = 0 WHERE grouping_id = " << group_id;
	sql::Statement* stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());
	delete stmt;
}

void WorkingTable::AssignSites(map<int, Team>& teams, map<int, Pool>& pools, DbConnectionDll::DbConnection& db)
{
	map<int, Team>::iterator tit = teams.begin();
	while (tit != teams.end()) {
		AssignSitesToTeam(tit, pools, db);
		tit++;
	}
}

void WorkingTable::AssignSitesToTeam(map<int, Team>::iterator team, map<int, Pool>& pools, DbConnectionDll::DbConnection& db)
{
	cout << "Assigning " << team->second.team_name << " ";
	vector<Distributor> distributors;

	map<int, Pool>::iterator pit = pools.begin();
	while (pit != pools.end()) {
		map<int, int>::iterator ptit = pit->second.teams.find(team->first);
		if (ptit != pit->second.teams.end()) {
			Distributor d(team, pit->first, ptit->second);
			distributors.push_back(d);
		}
		pit++;
	}

	if (0 == distributors.size()) {
		cout << endl;
		return;
	}
	
	stringstream query;
	query << "SELECT * FROM __workingtable";
	query << " WHERE team_id = " << team->first;
	query << " AND isDeal = 0";
	query << " AND pool_id = 0";
	query << " ORDER BY eac desc, days_left";
	sql::Statement* stmt = db.createStatement();
	sql::ResultSet* res = db.executeQuery(stmt, query.str());

	int nCnt = 0;
	vector<Distributor>::iterator dit = distributors.begin();
	bool bAllAtCapacity = true;
	do {
		// max_count = 0 means infinite
		if ((0 == dit->max_count) || (dit->current_count < dit->max_count)) {
			if (!res->next()) {
				cout << endl;
				return;
			}

			bAllAtCapacity = false;
			int site_id = res->getInt("site_id");
			int group_id = res->getInt("grouping_id");
			
			if (site_id > 0)
				AssignSite(site_id, pools[dit->pool_id], db);
			if (group_id > 0)
				AssignGroup(group_id, pools[dit->pool_id], db);

			if (0 == (++nCnt % 1000))
				cout << ".";
		}

		dit->current_count ++;

		if (distributors.end() == ++dit) {
			if (bAllAtCapacity)
				break;
			bAllAtCapacity = true;
			dit = distributors.begin();
		}

	} while (true);
	cout << endl;
}

void WorkingTable::AssignSite(int site_id, Pool& pool, DbConnectionDll::DbConnection& db)
{
	stringstream query;
	query << "UPDATE __workingtable SET pool_id = " << pool.id << " WHERE site_id = " << site_id;
	sql::Statement* stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());
	delete stmt;
}

/***
void WorkingTable::AssignSite(int site_id, Pool& pool, DbConnectionDll::DbConnection& db)
{
	stringstream query;
	query << "UPDATE sites SET pool_id = " << pool.id << " WHERE id = " << site_id;
	sql::Statement* stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());

	query.str("");
	if (pool.user_id > 0)
	{
		query << "UPDATE callbacks SET user_id = " << pool.user_id << " WHERE completed = 0 AND site_id = " << site_id;
		sql::Statement* stmt = db.createStatement();
		db.executeUpdate(stmt, query.str());
	}
	else
	{
		query << "DELETE FROM callbacks WHERE completed = 0 AND site_id = " << site_id;
		sql::Statement* stmt = db.createStatement();
		db.execute(stmt, query.str());
	}
	delete stmt;
}
***/

void WorkingTable::AssignGroup(int group_id, Pool& pool, DbConnectionDll::DbConnection& db)
{
	stringstream query;
	query << "UPDATE __workingtable SET pool_id = " << pool.id << " WHERE grouping_id = " << group_id;
	sql::Statement* stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());
	delete stmt;
}

/***
void WorkingTable::AssignGroup(int group_id, Pool& pool, DbConnectionDll::DbConnection& db)
{
	stringstream query;
	query << "UPDATE sites SET pool_id = " << pool.id << " WHERE grouping_id = " << group_id;
	sql::Statement* stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());

	query.str("");
	if (pool.user_id > 0)
	{
		query << "UPDATE callbacks SET user_id = " << pool.user_id << " WHERE completed = 0 AND site_id IN (";
		query << "SELECT id FROM sites WHERE grouping_id = " << group_id;
		query << ")";
		sql::Statement* stmt = db.createStatement();
		db.executeUpdate(stmt, query.str());
	}
	else
	{
		query << "DELETE FROM callbacks WHERE completed = 0 AND site_id IN (";
		query << "SELECT id FROM sites WHERE grouping_id = " << group_id;
		query << ")";
		sql::Statement* stmt = db.createStatement();
		db.execute(stmt, query.str());
	}
	delete stmt;
}
***/

void WorkingTable::SetPoolWeightsForTeam(map<int, Team>::iterator team, map<int, Pool>& pools)
{
	map<int, int>::iterator _p = team->second.pools.begin();
	while (_p != team->second.pools.end()) {
		_p->second = pools[_p->first].team_weight;
		_p++;
	}
}

void WorkingTable::ApplyChanges(DbConnectionDll::DbConnection&db) {
	ApplySiteChanges(db);
	ApplyGroupChanges(db);
	UpdateCallbacks(db);
	
	//RemoveUserIdsFromCallbacks(db);
	//ApplySiteCallbackChanges(db);
	//ApplyGroupCallbackChanges(db);
	//DeleteOrphanedCallbacks(db);
}

void WorkingTable::ApplySiteChanges(DbConnectionDll::DbConnection&db) {
	cout << "Applying Site Changes ..." << endl;

	// update pools
	stringstream query;
	query << "UPDATE __workingtable w";
	query << " LEFT JOIN sites s ON w.site_id = s.id";
	query << " SET s.pool_id = w.pool_id";
	query << " WHERE !w.isDeal";
	query << " AND w.site_id > 0";
	sql::Statement* stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());

	// update segments
	query.str("");
	query << "UPDATE __workingtable w";
	query << " LEFT JOIN sites s ON w.site_id = s.id";
	query << " SET s.segment = w.team_id";
	query << " WHERE w.site_id > 0";
	db.executeUpdate(stmt, query.str());
	delete stmt;
}

void WorkingTable::ApplyGroupChanges(DbConnectionDll::DbConnection&db) {
	cout << "Applying Group Changes ..." << endl;

	// update pools
	stringstream query;
	query << "UPDATE __workingtable w";
	query << " LEFT JOIN sites s ON w.grouping_id = s.grouping_id";
	query << " SET s.pool_id = w.pool_id";
	query << " WHERE !w.isDeal";
	query << " AND w.grouping_id > 0";
	sql::Statement* stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());

	// update segments
	query.str("");
	query << "UPDATE __workingtable w";
	query << " LEFT JOIN sites s ON w.grouping_id = s.grouping_id";
	query << " SET s.segment = w.team_id";
	query << " WHERE w.grouping_id > 0";
	db.executeUpdate(stmt, query.str());
	delete stmt;
}

void WorkingTable::UpdateCallbacks(DbConnectionDll::DbConnection&db) {
	cout << "Updating Callbacks ..." << endl;

	stringstream query;
	query << "SET innodb_lock_wait_timeout = 1000";
	sql::Statement* stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());

	query.str("");
	query << "UPDATE callbacks cb";
	query << " LEFT JOIN sites s ON s.id = cb.site_id";
	query << " LEFT JOIN users u on u.primary_pool_id = s.pool_id";
	query << " SET cb.user_id = u.id";
	query << " WHERE !cb.completed";
	query << " AND s.pool_id > 0";
	query << " AND u.id IS NOT NULL";
	db.executeUpdate(stmt, query.str());

	/***
	Add in where ... AND u.id IS NOT NULL
	This should not reset cb users where there is no primary pool
	Then add a new query that removes callbacks where there is no primary pool and the user isn't associated with the general pool
	***/

/***
The next few queries where replaced by a better method (below) by RKM on 18/04/2017

	query.str("");
	query << "DROP TABLE IF EXISTS tmp_cb";
	db.execute(stmt, query.str());
	query.str("");
	query << "CREATE TABLE tmp_cb (id INT)";
	db.execute(stmt, query.str());
	query.str("");
	query << "INSERT INTO tmp_cb (";
	query << " SELECT cb.id FROM sites s";
	query << " LEFT JOIN callbacks cb ON cb.site_id = s.id";
	query << " LEFT JOIN users u ON cb.user_id = u.id";
	query << " LEFT JOIN pool_user pu ON (pu.pool_id = s.pool_id AND pu.user_id = cb.user_id)";
	query << " WHERE cb.site_id IS NOT NULL";
	query << " AND pu.user_id IS NULL";
	query << " )";
	db.execute(stmt, query.str());
	query.str("");
	query << "DELETE FROM callbacks";
	query << " WHERE id IN (";
	query << " SELECT id FROM tmp_cb";
	query << " )";
	query.str("");
	query << "DROP TABLE IF EXISTS tmp_cb";
	db.execute(stmt, query.str());
***/

	query.str("");
	query << " DELETE cb FROM sites s";
	query << " LEFT JOIN callbacks cb ON cb.site_id = s.id";
	query << " LEFT JOIN users u ON cb.user_id = u.id";
	query << " LEFT JOIN pool_user pu ON (pu.pool_id = s.pool_id AND pu.user_id = cb.user_id)";
	query << " WHERE cb.site_id IS NOT NULL";
	query << " AND !cb.completed";
	query << " AND pu.user_id IS NULL";
	db.execute(stmt, query.str());

	/***
	RKM Added 2016-11-17
	We had a problem where sites with callbacks were transferred into pools with no primary user.
	In these cases the callback user_id was set to null and laravel constructs like $site->Callback->User->id
	errored because User was not an object.
	***/
	query.str("");
	query << "DELETE FROM callbacks";
	query << " WHERE user_id IS NULL";
	query << " AND !completed";
	db.executeUpdate(stmt, query.str());

	query.str("");
	query << "SET innodb_lock_wait_timeout = 50";
	db.executeUpdate(stmt, query.str());

	delete stmt;
}

void WorkingTable::RemoveUserIdsFromCallbacks(DbConnectionDll::DbConnection&db) {
	cout << "Removing User Ids From Callbacks ..." << endl;

	stringstream query;
	query << "UPDATE callbacks";
	query << " SET user_id = 0";
	sql::Statement* stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());
	delete stmt;
}

void WorkingTable::ApplySiteCallbackChanges(DbConnectionDll::DbConnection&db) {
	cout << "Applying Site Callback Changes ..." << endl;

	stringstream query;
	query << "UPDATE __workingtable w";
	query << " LEFT JOIN callbacks cb ON w.site_id = cb.site_id";
	query << " SET cb.pool_id = w.pool_id";
	query << " WHERE w.site_id > 0";
	sql::Statement* stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());
	delete stmt;
}

void WorkingTable::ApplyGroupCallbackChanges(DbConnectionDll::DbConnection&db) {
	cout << "Applying Group Callback Changes ..." << endl;

	stringstream query;
	query << "UPDATE __workingtable w";
	query << " LEFT JOIN site s ON w.grouping_id = s.grouping_id";
	query << " LEFT JOIN callbacks cb ON cb.site_id = s.id";
	query << " SET cb.pool_id = w.pool_id";
	query << " WHERE w.grouping_id > 0";
	sql::Statement* stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());
	delete stmt;

}

void WorkingTable::DeleteOrphanedCallbacks(DbConnectionDll::DbConnection& db) {
	cout << "Deleting Orphaned Callbacks ..." << endl;

	stringstream query;
	query << "DELETE FROM callbacks";
	query << " WHERE user_id = 0";
	sql::Statement* stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());
	delete stmt;
}

void WorkingTable::SetDaysLeft(DbConnectionDll::DbConnection& db) {
	stringstream query;
	sql::Statement* stmt = db.createStatement();

	// Find deal status of single sites
	cout << "Finding days_left for Single Sites ..." << endl;
	query << "UPDATE __workingtable w";
	query << " LEFT JOIN";
	query << "(";
	query << " SELECT s.id AS site_id,";
	query << " MIN(c.renewal_date) AS next_renewal_date";
	query << " FROM sites s";
	query << " LEFT JOIN meters m ON s.id = m.site_id";
	query << " LEFT JOIN contracts c ON m.contract_id = c.id";
	query << " WHERE !m.deenergised";
	query << " AND c.renewal_date > '0000-00-00'";
	query << " GROUP BY s.id";
	query << ") sq";
	query << " ON w.site_id = sq.site_id";
	query << " SET w.days_left = DATEDIFF(sq.next_renewal_date, CURDATE())";
	query << " WHERE w.site_id > 0";
	db.executeUpdate(stmt, query.str());

	// Find deal status of site groups
	cout << "Finding days_left for Site Groups ..." << endl;
	query.str("");
	query << "UPDATE __workingtable w";
	query << " LEFT JOIN";
	query << "(";
	query << " SELECT s.grouping_id AS grouping_id,";
	query << " MIN(c.renewal_date) AS next_renewal_date";
	query << " FROM sites s";
	query << " LEFT JOIN meters m ON s.id = m.site_id";
	query << " LEFT JOIN contracts c ON m.contract_id = c.id";
	query << " WHERE !m.deenergised";
	query << " AND c.renewal_date > '0000-00-00'";
	query << " GROUP BY s.grouping_id";
	query << ") sq";
	query << " ON w.grouping_id = sq.grouping_id";
	query << " SET w.days_left = DATEDIFF(sq.next_renewal_date, CURDATE())";
	query << " WHERE w.grouping_id > 0";
	db.executeUpdate(stmt, query.str());

	cout << "Setting NULL days_left to 0..." << endl;
	query.str("");
	query << "UPDATE __workingtable";
	query << " SET days_left = 0";
	query << " WHERE days_left IS NULL";
	db.executeUpdate(stmt, query.str());

	delete stmt;
}

void WorkingTable::SetCallbackFlag(DbConnectionDll::DbConnection&db) {
	stringstream query;
	sql::Statement* stmt = db.createStatement();

	// Tidy up the callback flag
	cout << "Setting the Callback Flag ..." << endl;
	
	query << "UPDATE __workingtable w";
	query << " LEFT JOIN callbacks cb ON cb.site_id = w.site_id";
	query << " SET w.hasCallback = TRUE";
	query << " WHERE cb.site_id IS NOT NULL";
	// query << " AND cb.completed = 0"; RKM 207-02-16 ???
	stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());

	/***
	query << "UPDATE __workingtable";
	query << " SET hasCallback = TRUE";
	query << " WHERE hasCallback IS NOT NULL";
	stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());
	query.str("");
	query << "UPDATE __workingtable";
	query << " SET hasCallback = FALSE";
	query << " WHERE hasCallback IS NULL";
	stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());
	***/

	delete stmt;
}

void WorkingTable::FindDealStatus(DbConnectionDll::DbConnection&db) {
	stringstream query;
	sql::Statement* stmt = db.createStatement();

	// Find deal status of single sites
	cout << "Finding Deal Status of Single Sites ..." << endl;
	query << "UPDATE __workingtable w";
	query << " LEFT JOIN meters m ON m.site_id = w.site_id";
	query << " LEFT JOIN contracts c ON m.contract_id = c.id";
	query << " SET w.isDeal = TRUE";
	query << " WHERE w.site_id > 0";
	query << " AND !m.deenergised";
	query << " AND c.status NOT IN(0, 8, 9, 13, 14)";
	stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());

	// Find deal status of site groups
	cout << "Finding Deal Status of Site Groups ..." << endl;
	query.str("");
	query << "UPDATE __workingtable w";
	query << " LEFT JOIN sites s ON s.grouping_id = w.grouping_id";
	query << " LEFT JOIN meters m ON m.site_id = s.id";
	query << " LEFT JOIN contracts c ON m.contract_id = c.id";
	query << " SET w.isDeal = TRUE";
	query << " WHERE w.site_id = 0";
	query << " AND !m.deenergised";
	query << " AND c.status NOT IN(0, 8, 9, 13, 14)";
	stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());

	delete stmt;
}

void WorkingTable::FindSupplier(DbConnectionDll::DbConnection&db) {
	stringstream query;
	sql::Statement* stmt = db.createStatement();

	// Find supplier of single sites
	cout << "Finding Supplier of Single Sites ..." << endl;
	query.str(""); // Stage 1: Mark all sites supplied by Npower only as supplier 1
	query << "UPDATE __workingtable w";
	query << " LEFT JOIN meters m ON m.site_id = w.site_id";
	query << " LEFT JOIN contracts c ON m.contract_id = c.id";
	query << " SET w.supplier = 1"; // Npower
	query << " WHERE w.site_id > 0";
	query << " AND w.supplier = 0";
	query << " AND !m.deenergised";
	query << " AND c.supplier_id IN (1, 6)";
	query << " AND w.isConfirmedByLastRefresh";
	stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());
	query.str(""); // Stage 2: Mark all sites supplied by Eon only as supplier 2
	query << "UPDATE __workingtable w";
	query << " LEFT JOIN meters m ON m.site_id = w.site_id";
	query << " LEFT JOIN contracts c ON m.contract_id = c.id";
	query << " SET w.supplier = 2"; // Eon
	query << " WHERE w.site_id > 0";
	query << " AND w.supplier = 0";
	query << " AND !m.deenergised";
	query << " AND c.supplier_id = 2";
	stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());
	query.str(""); // Stage 3: Mark all sites supplied by British Gas only as supplier 3
	query << "UPDATE __workingtable w";
	query << " LEFT JOIN meters m ON m.site_id = w.site_id";
	query << " LEFT JOIN contracts c ON m.contract_id = c.id";
	query << " SET w.supplier = 3"; // BG
	query << " WHERE w.site_id > 0";
	query << " AND w.supplier = 0";
	query << " AND !m.deenergised";
	query << " AND c.supplier_id = 3";
	stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());
	query.str(""); // Stage 4: Mark all sites supplied by multiple suppliers as supplier 0
	query << "UPDATE __workingtable w";
	query << " LEFT JOIN meters m ON m.site_id = w.site_id";
	query << " LEFT JOIN contracts c ON m.contract_id = c.id";
	query << " SET w.supplier = 0"; // Other
	query << " WHERE w.site_id > 0";
	//query << " AND w.supplier != 1"; // Keep supplier as Npower even of there are meters supplied by other suppliers
	query << " AND w.supplier != c.supplier_id";
	query << " AND !m.deenergised";
	stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());

	/***
	query.str(""); // Stage 5: Mark all sites supplied by any other supplier as supplier 0
	query << "UPDATE __workingtable w";
	query << " LEFT JOIN meters m ON m.site_id = w.site_id";
	query << " LEFT JOIN contracts c ON m.contract_id = c.id";
	query << " SET w.supplier = 0"; // Other
	query << " WHERE w.site_id > 0";
	query << " AND !m.deenergised";
	query << " AND c.supplier_id NOT IN (1, 2)";
	stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());
	***/

	// Find supplier of site groups
	cout << "Finding Supplier of Site Groups ..." << endl;
	query.str(""); // Stage 1: Mark all sites supplied by Npower only as supplier 1
	query << "UPDATE __workingtable w";
	query << " LEFT JOIN sites s ON s.grouping_id = w.grouping_id";
	query << " LEFT JOIN meters m ON m.site_id = s.id";
	query << " LEFT JOIN contracts c ON m.contract_id = c.id";
	query << " SET w.supplier = 1"; // Npower
	query << " WHERE w.site_id = 0";
	query << " AND w.supplier = 0";
	query << " AND !m.deenergised";
	query << " AND c.supplier_id IN (1, 6)";
	query << " AND w.isConfirmedByLastRefresh";
	stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());
	query.str(""); // Stage 2: Mark all sites supplied by Eon only as supplier 2
	query << "UPDATE __workingtable w";
	query << " LEFT JOIN sites s ON s.grouping_id = w.grouping_id";
	query << " LEFT JOIN meters m ON m.site_id = s.id";
	query << " LEFT JOIN contracts c ON m.contract_id = c.id";
	query << " SET w.supplier = 2"; // Eon
	query << " WHERE w.site_id = 0";
	query << " AND w.supplier = 0";
	query << " AND !m.deenergised";
	query << " AND c.supplier_id = 2";
	stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());
	query.str(""); // Stage 3: Mark all sites supplied by British Gas only as supplier 3
	query << "UPDATE __workingtable w";
	query << " LEFT JOIN sites s ON s.grouping_id = w.grouping_id";
	query << " LEFT JOIN meters m ON m.site_id = s.id";
	query << " LEFT JOIN contracts c ON m.contract_id = c.id";
	query << " SET w.supplier = 3"; // BG
	query << " WHERE w.site_id = 0";
	query << " AND w.supplier = 0";
	query << " AND !m.deenergised";
	query << " AND c.supplier_id = 3";
	stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());
	query.str(""); // Stage 4: Mark all sites supplied by by multiple suppliers as supplier 0
	query << "UPDATE __workingtable w";
	query << " LEFT JOIN sites s ON s.grouping_id = w.grouping_id";
	query << " LEFT JOIN meters m ON m.site_id = s.id";
	query << " LEFT JOIN contracts c ON m.contract_id = c.id";
	query << " SET w.supplier = 0"; // Other
	query << " WHERE w.site_id = 0";
	//query << " AND w.supplier != 1"; // Keep supplier as Npower even of there are meters supplied by other suppliers
	query << " AND w.supplier != c.supplier_id";
	query << " AND !m.deenergised";
	stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());

	/***
	query.str(""); // Stage 4: Mark all sites supplied by any other supplier as supplier 0
	query << "UPDATE __workingtable w";
	query << " LEFT JOIN sites s ON s.grouping_id = w.grouping_id";
	query << " LEFT JOIN meters m ON m.site_id = s.id";
	query << " LEFT JOIN contracts c ON m.contract_id = c.id";
	query << " SET w.supplier = 0"; // Other
	query << " WHERE w.site_id = 0";
	query << " AND !m.deenergised";
	query << " AND c.supplier_id NOT IN (1, 2)";
	stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());
	***/

	delete stmt;
}

void WorkingTable::FlagHalfHourlies(DbConnectionDll::DbConnection&db) {
	stringstream query;
	sql::Statement* stmt = db.createStatement();

	// Flag single site half hourlies
	cout << "Finding Single Site Half Hourlies ..." << endl;
	query << "UPDATE __workingtable w";
	query << " LEFT JOIN meters m ON m.site_id = w.site_id";
	query << " SET w.isHalfHourly = TRUE";
	query << " WHERE w.site_id > 0";
	query << " AND !m.deenergised";
	query << " AND SUBSTR(m.top_line, 1, 2) = '00'";
	stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());

	// Flag site group half hourlies
	cout << "Finding Site Group Half Hourlies ..." << endl;
	query.str("");
	query << "UPDATE __workingtable w";
	query << " LEFT JOIN sites s ON s.grouping_id = w.grouping_id";
	query << " LEFT JOIN meters m ON m.site_id = s.id";
	query << " SET w.isHalfHourly = TRUE";
	query << " WHERE w.site_id = 0";
	query << " AND !m.deenergised";
	query << " AND SUBSTR(m.top_line, 1, 2) = '00'";
	stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());

	delete stmt;
}

void WorkingTable::FlagMaxDemands(DbConnectionDll::DbConnection&db) {
	stringstream query;
	sql::Statement* stmt = db.createStatement();

	// Flag single site max demands
	cout << "Finding Single Site Max Demands ..." << endl;
	query << "UPDATE __workingtable w";
	query << " LEFT JOIN meters m ON m.site_id = w.site_id";
	query << " SET w.isMaxDemand = TRUE";
	query << " WHERE w.site_id > 0";
	query << " AND !m.deenergised";
	query << " AND SUBSTR(m.top_line, 1, 2) IN ('05', '06', '07', '08')";
	stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());

	// Flag site group max demands
	cout << "Finding Site Group Max Demands ..." << endl;
	query.str("");
	query << "UPDATE __workingtable w";
	query << " LEFT JOIN sites s ON s.grouping_id = w.grouping_id";
	query << " LEFT JOIN meters m ON m.site_id = s.id";
	query << " SET w.isMaxDemand = TRUE";
	query << " WHERE w.site_id = 0";
	query << " AND !m.deenergised";
	query << " AND SUBSTR(m.top_line, 1, 2) IN ('05', '06', '07', '08')";
	stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());

	delete stmt;
}

void WorkingTable::FlagRelatedMeters(DbConnectionDll::DbConnection&db) {
	stringstream query;
	sql::Statement* stmt = db.createStatement();

	// Flag single site related meters
	cout << "Finding Single Site Related Meters ..." << endl;
	query << "UPDATE __workingtable w";
	query << " LEFT JOIN meters m ON m.site_id = w.site_id";
	query << " SET w.isRelated = TRUE";
	query << " WHERE w.site_id > 0";
	query << " AND !m.deenergised";
	query << " AND SUBSTR(m.top_line, 3, 1) = '5'";
	stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());

	// Flag site group related meters
	cout << "Finding Site Group Related Meters ..." << endl;
	query.str("");
	query << "UPDATE __workingtable w";
	query << " LEFT JOIN sites s ON s.grouping_id = w.grouping_id";
	query << " LEFT JOIN meters m ON m.site_id = s.id";
	query << " SET w.isRelated = TRUE";
	query << " WHERE w.site_id = 0";
	query << " AND !m.deenergised";
	query << " AND SUBSTR(m.top_line, 3, 1) = '5'";
	stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());

	delete stmt;
}

void WorkingTable::FlagRefreshConfirmedMeters(DbConnectionDll::DbConnection&db, string wtFieldName, string refresh_date) {
	//string strRefresh = "REFRESH: 2016-10-05";
	//string strRefresh = "REFRESH: 2016-12-01";
	//string strRefresh = "REFRESH: 2017-01-16";
	string refresh_signature = "REFRESH: " + refresh_date;
	stringstream query;
	sql::Statement* stmt = db.createStatement();

	// Flag single site refresh confirmed meters
	cout << "Finding Single Site Refresh Confirmed Meters ..." << endl;
	query << "UPDATE __workingtable w";
	query << " LEFT JOIN notes n ON n.site_id = w.site_id";
	query << " SET w." << wtFieldName << " = TRUE";
	query << " WHERE w.site_id > 0";
	query << " AND n.text LIKE '" << refresh_signature << "%'";
	stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());

	// Flag site group refresh confirmed meters
	cout << "Finding Site Group Refresh Confirmed Meters ..." << endl;
	query.str("");
	query << "UPDATE __workingtable w";
	query << " LEFT JOIN sites s ON s.grouping_id = w.grouping_id";
	query << " LEFT JOIN notes n ON n.site_id = s.id";
	query << " SET w.isConfirmedByLastRefresh = TRUE";
	query << " WHERE w.site_id = 0";
	query << " AND n.text LIKE '" << refresh_signature << "%'";
	stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());

	delete stmt;
}

void WorkingTable::FlagFlexibleTypeContracts(DbConnectionDll::DbConnection&db, string contract_type) {
	stringstream query;
	sql::Statement* stmt = db.createStatement();

	string setColumn;
	int type_id;
	if ("Flexible" == contract_type) { setColumn = "w.isFlexible"; type_id = 1; }
	if ("Variable" == contract_type) { setColumn = "w.isVariable"; type_id = 2; }
	if ("Default" == contract_type) { setColumn = "w.isDefault"; type_id = 3; }
	if ("Deemed" == contract_type) { setColumn = "w.isDeemed"; type_id = 4; }
	if ("Tariff" == contract_type) { setColumn = "w.isTariff"; type_id = 5; }
	if (0 == setColumn.length()) return;

	// Flag single site meters on flexible type contracts
	cout << "Finding Single Site Flexible Meters ..." << endl;
	query << "UPDATE __workingtable w";
	query << " LEFT JOIN meters m on m.site_id = w.site_id";
	query << " LEFT JOIN contracts c on c.id = m.contract_id";
	query << " SET " << setColumn << " = TRUE";
	query << " WHERE w.site_id > 0";
	query << " AND !m.deenergised";
	query << " AND c.type = " << type_id;
	stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());

	// Flag site group meters on flexible type contracts
	cout << "Finding Site Group Flexible Etc Meters ..." << endl;
	query.str("");
	query << "UPDATE __workingtable w";
	query << " LEFT JOIN sites s ON s.grouping_id = w.grouping_id";
	query << " LEFT JOIN meters m on m.site_id = s.id";
	query << " LEFT JOIN contracts c on c.id = m.contract_id";
	query << " SET " << setColumn << " = TRUE";
	query << " WHERE w.site_id = 0";
	query << " AND !m.deenergised";
	query << " AND c.type = " << type_id;
	stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());

	delete stmt;
}

void WorkingTable::CalculateConsumption(DbConnectionDll::DbConnection&db) {
	stringstream query;
	sql::Statement* stmt = db.createStatement();

	// Calculate consumption of single sites
	cout << "Calculating EAC of Single Sites ..." << endl;
	query << "UPDATE __workingtable w";
	query << " SET w.eac = (";
	query << " SELECT sum(c.eac_aq)";
	query << " FROM sites s";
	query << " LEFT JOIN meters m ON m.site_id = s.id";
	query << " LEFT JOIN contracts c ON m.contract_id = c.id";
	query << " WHERE s.id = w.site_id";
	query << " AND LENGTH(TRIM(m.mpr)) = 0";
	query << " AND !m.deenergised)";
	query << " WHERE w.site_id > 0";
	db.executeUpdate(stmt, query.str());

	// Calculate aq of single sites
	cout << "Calculating AQ of Single Sites ..." << endl;
	query.str("");
	query << "UPDATE __workingtable w";
	query << " SET w.aq = (";
	query << " SELECT sum(c.eac_aq)";
	query << " FROM sites s";
	query << " LEFT JOIN meters m ON m.site_id = s.id";
	query << " LEFT JOIN contracts c ON m.contract_id = c.id";
	query << " WHERE s.id = w.site_id";
	query << " AND LENGTH(TRIM(m.mpr)) > 0";
	query << " AND !m.deenergised)";
	query << " WHERE w.site_id > 0";
	db.executeUpdate(stmt, query.str());

	// Calculate consumption of site groups
	cout << "Calculating EAC of Site Groups ..." << endl;
	query.str("");
	query << "UPDATE __workingtable w";
	query << " LEFT JOIN sites s ON s.grouping_id = w.grouping_id";
	query << " SET w.eac = (";
	query << " SELECT sum(c.eac_aq)";
	query << " FROM sites s";
	query << " LEFT JOIN meters m ON m.site_id = s.id";
	query << " LEFT JOIN contracts c ON m.contract_id = c.id";
	query << " WHERE s.grouping_id = w.grouping_id";
	query << " AND LENGTH(TRIM(m.mpr)) = 0";
	query << " AND !m.deenergised)";
	query << " WHERE w.grouping_id > 0";
	db.executeUpdate(stmt, query.str());

	// Calculate aq of site groups
	cout << "Calculating AQ of Site Groups ..." << endl;
	query.str("");
	query << "UPDATE __workingtable w";
	query << " LEFT JOIN sites s ON s.grouping_id = w.grouping_id";
	query << " SET w.aq = (";
	query << " SELECT sum(c.eac_aq)";
	query << " FROM sites s";
	query << " LEFT JOIN meters m ON m.site_id = s.id";
	query << " LEFT JOIN contracts c ON m.contract_id = c.id";
	query << " WHERE s.grouping_id = w.grouping_id";
	query << " AND LENGTH(TRIM(m.mpr)) > 0";
	query << " AND !m.deenergised)";
	query << " WHERE w.grouping_id > 0";
	db.executeUpdate(stmt, query.str());

	// Tidy up NULL values
	cout << "Tidying null EACS" << endl;
	query.str("");
	query << "UPDATE __workingtable w";
	query << " SET w.eac = 0";
	query << " WHERE w.eac is NULL";
	db.executeUpdate(stmt, query.str());
	cout << "Tidying null AQS" << endl;
	query.str("");
	query << "UPDATE __workingtable w";
	query << " SET w.aq = 0";
	query << " WHERE w.aq is NULL";
	db.executeUpdate(stmt, query.str());

	delete stmt;
}

void WorkingTable::FlagDeEnergisedMeters(DbConnectionDll::DbConnection&db) {
	stringstream query;
	sql::Statement* stmt = db.createStatement();

	// Flag single sites with de-energised meters
	cout << "Finding Single Sites With De-Energised Meters ..." << endl;
	query << "UPDATE __workingtable w";
	query << " LEFT JOIN meters m ON m.site_id = w.site_id";
	query << " SET w.isDeEnergised = TRUE";
	query << " WHERE w.site_id > 0";
	query << " AND m.deenergised";
	stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());

	// Flag site groups with de-energised meters
	cout << "Finding Site Groups With De-Energised Meters ..." << endl;
	query.str("");
	query << "UPDATE __workingtable w";
	query << " LEFT JOIN sites s ON s.grouping_id = w.grouping_id";
	query << " LEFT JOIN meters m ON m.site_id = s.id";
	query << " SET w.isDeEnergised = TRUE";
	query << " WHERE w.site_id = 0";
	query << " AND m.deenergised";
	stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());

	// Now Un-flag single sites where not all meters are de-energised
	cout << "Un-flagging Single Sites Where Not All Meters Are De-Energised ..." << endl;
	query.str("");
	query << "UPDATE __workingtable w";
	query << " LEFT JOIN meters m ON m.site_id = w.site_id";
	query << " SET w.isDeEnergised = FALSE";
	query << " WHERE w.site_id > 0";
	query << " AND !m.deenergised";
	stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());

	// Un-flag site groups where not all meters are de-energised
	cout << "Un-flagging Site Groups Where Not All Meters Are De-Energised ..." << endl;
	query.str("");
	query << "UPDATE __workingtable w";
	query << " LEFT JOIN sites s ON s.grouping_id = w.grouping_id";
	query << " LEFT JOIN meters m ON m.site_id = s.id";
	query << " SET w.isDeEnergised = FALSE";
	query << " WHERE w.site_id = 0";
	query << " AND !m.deenergised";
	stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());

	delete stmt;
}

void WorkingTable::FindSource(DbConnectionDll::DbConnection&db) {
	stringstream query;
	sql::Statement* stmt = db.createStatement();

	// Set source of single sites to Web(1) is there is a web_customer_id
	cout << "Finding Site Sources ..." << endl;
	query << "UPDATE __workingtable w";
	query << " LEFT JOIN sites s ON s.id = w.site_id";
	query << " SET w.source = 1";
	query << " WHERE length(s.web_customer_id) > 0";
	query << " AND w.site_id > 0";
	stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());

	// Web leads are never associated
	delete stmt;
}

void WorkingTable::CountNotes(DbConnectionDll::DbConnection&db) {
	stringstream query;
	sql::Statement* stmt = db.createStatement();

	// Set source of single sites to Web(1) is there is a web_customer_id
	cout << "Counting Notes ..." << endl;
	query << "UPDATE __workingtable w";
	query << " SET w.numNotes =";
	query << " (";
	query << " SELECT count(1) FROM notes n";
	query << " WHERE n.site_id = w.site_id";
	query << " )";
	query << " WHERE w.site_id > 0";
	stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());

	query.str("");
	query << "UPDATE __workingtable w";
	query << " SET w.numNotes =";
	query << " (";
	query << " SELECT count(1) FROM notes n";
	query << " LEFT JOIN sites s ON s.id = n.site_id";
	query << " WHERE s.grouping_id = w.grouping_id";
	query << " )";
	query << " WHERE w.grouping_id > 0";
	stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());

	delete stmt;
}
