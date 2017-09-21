// WATTSAPP-TopUp.cpp : Defines the entry point for the console application.
//

#pragma comment(lib, "C:/Users/Rob/Documents/Visual Studio 2015/Projects/Utilities/Debug/Utilities.lib")
#pragma comment(lib, "C:/Users/Rob/Documents/Visual Studio 2015/Projects/Formatter/Debug/Formatter.lib")
#pragma comment(lib, "C:/Users/Rob/Documents/Visual Studio 2015/Projects/DbConnection/Debug/DbConnection.lib")
#pragma comment(lib, "C:/Users/Rob/Documents/Visual Studio 2015/Projects/DbConnection/DbConnection/mysqlcppconn.lib")

#include "stdafx.h"
#include <iostream>
#include <fstream>
#include <map>
#include <iomanip>
#include <ctime>
#include "C:/Users/Rob/Documents/Visual Studio 2015/Projects/Utilities/Utilities/Utilities.h"
#include "C:/Users/Rob/Documents/Visual Studio 2015/Projects/Formatter/Formatter/Formatter.h"
#include "C:/Users/Rob/Documents/Visual Studio 2015/Projects/DbConnection/DbConnection/DbConnection.h"

#include "Janitor.h"
#include "WorkingTable.h"
#include "Team.h"
#include "Pool.h"
#include "TableSafe.h"
#include "ConnectionDetails.h"

using namespace std;
int GetTeams(map<int, Team>& teams, DbConnectionDll::DbConnection& db);
int GetPools(map<int, Pool>& pools, map<int, Team>& teams, DbConnectionDll::DbConnection& db);
int GetRules(map<int, Rule>& rules, DbConnectionDll::DbConnection& db);
void GeneratePoolStatistics(map<int, Team>& team, DbConnectionDll::DbConnection& db);
void LogPoolStatistics(map<int, Pool>& pools, map<int, Team>& team, ofstream& outf);
bool BackupDb(tm& now, ConnectionDetails& cd);
string FindDbBackupFolder();
void help(char* argv[]);

int main(int argc, char* argv[])
{
	if (argc > 1 && !strcmp(argv[1], "/?")) {
		help(argv);
		return EXIT_SUCCESS;
	}

	ConnectionDetails cd(argc, argv);

	//Are we topping up the corporate database?
	bool bCorp = false;
	if ((argc > 1 && !strcmp(argv[1], "/CORP")) || (argc > 2 && !strcmp(argv[2], "/CORP"))) {
		cd.dbname = "corp_" + cd.dbname;
		bCorp = true;
	}

	//Find the date & time
	tm now;
	time_t t = time(0);   // get time now
	localtime_s(&now, &t);

	//Backup the database
	if (argc > 1 && strcmp(argv[1], "/COUNTS")) {
		if (!BackupDb(now, cd)) {
			cout << "No valid backup folder found" << endl;
			return EXIT_FAILURE;
		}
	}

	DbConnectionDll::DbConnection db(cd.dbhost, cd.dbuser, cd.dbpwd, cd.dbname);
	//DbConnectionDll::DbConnection db("server", "fineit", "makaton", dbName);
	//DbConnectionDll::DbConnection db("192.168.100.4", "root", "makaton", dbName);
	//DbConnectionDll::DbConnection db("localhost", "root", "makaton", dbName);
	//DbConnectionDll::DbConnection db("192.168.67.27", "root", "makaton", dbName);

	if (argc > 1 && !strcmp(argv[1], "/RECREATESEGMENTATIONRULES")) {
		TableSafe::RecreateSegmentationRules(db);
		return EXIT_SUCCESS;
	}
	if (argc > 1 && !strcmp(argv[1], "/RECREATETEAMS")) {
		TableSafe::RecreateTeams(db);
		return EXIT_SUCCESS;
	}
	if (argc > 1 && !strcmp(argv[1], "/V")) {
		cout << "VERSION: 1.20" << endl;
		return EXIT_SUCCESS;
	}

	// Open the job log
	stringstream filetime;
	filetime << now.tm_year + 1900 << setfill('0') << setw(2) << now.tm_mon + 1 << setw(2) << now.tm_mday << setw(2) << now.tm_hour << setw(2) << now.tm_min << setw(2) << now.tm_sec;
	string corpStr = (bCorp ? "corp." : "");
	ofstream outf("topup3." + corpStr + filetime.str() + ".job.log.csv");

	if ((1 == argc) || (strcmp(argv[1], "/ASSIGNONLY") && strcmp(argv[1], "/COUNTS")))
		Janitor::PerformRegularMaintenance(db);
	
	map<int, Team> teams;
	map<int, Pool> pools;
	map<int, Rule> rules;
	GetTeams(teams, db);
	GetPools(pools, teams, db);
	GetRules(rules, db);

	if ((1 == argc) || (strcmp(argv[1], "/ASSIGNONLY") && strcmp(argv[1], "/COUNTS"))) {
		WorkingTable::CreateTable(db);
		WorkingTable::Populate(db);
		WorkingTable::BuildIndexes(db);
		WorkingTable::Segment(rules, db, bCorp);
	}
	GeneratePoolStatistics(teams, db);
	LogPoolStatistics(pools, teams, outf);

	if ((1 == argc) || (strcmp(argv[1], "/TOPUP") && strcmp(argv[1], "/ASSIGNONLY"))) {
		outf.close();
		return EXIT_SUCCESS; 
	}

	WorkingTable::FreeNonMatchingSites(teams, db);
	WorkingTable::AssignSites(teams, pools, db);
	GeneratePoolStatistics(teams, db);
	LogPoolStatistics(pools, teams, outf);
	WorkingTable::ApplyChanges(db);

	outf.close();
	return EXIT_SUCCESS;
}

int GetTeams(map<int, Team>& teams, DbConnectionDll::DbConnection& db)
{
	cout << "Getting Teams ..." << endl;
	stringstream query;
	query << "SELECT * FROM teams";
	sql::Statement* stmt = db.createStatement();
	sql::ResultSet* res = db.executeQuery(stmt, query.str());
	while (res->next())
	{
		Team t(res);
		teams.insert(pair<int, Team>(t.id, t));
	}
	delete res;
	delete stmt;
	return (int)teams.size();
}

int GetPools(map<int, Pool>& pools, map<int, Team>& teams, DbConnectionDll::DbConnection& db)
{
	cout << "Getting Pools ..." << endl;
	stringstream query;
	query << "SELECT pt.*, u.id AS user_id, p.pool_name";
	query << " FROM pool_team pt";
	query << " LEFT JOIN users u ON pt.pool_id = u.primary_pool_id";
	query << " LEFT JOIN pools p on pt.pool_id = p.id";
	sql::Statement* stmt = db.createStatement();
	sql::ResultSet* res = db.executeQuery(stmt, query.str());
	while (res->next())
	{
		map<int, Pool>::iterator pit = pools.find(res->getInt("pool_id"));
		if (pit == pools.end())
		{
			Pool p(res, teams);
			pools.insert(pair<int, Pool>(p.id, p));
		}
		else
			pit->second.teams.insert(pair<int, int>(res->getInt("team_id"), res->getInt("max_records")));
	}

	/***
	map<int, Pool>::iterator pit = pools.begin();
	while (pit != pools.end()) {
		pit->second.CalculateTotalTeamWeight();
		pit++;
	}
	***/

	delete res;
	delete stmt;
	return (int)pools.size();
}

int GetRules(map<int, Rule>& rules, DbConnectionDll::DbConnection& db)
{
	cout << "Getting Rules ..." << endl;
	stringstream query;
	query << "SELECT * FROM segmentation_rules";
	sql::Statement* stmt = db.createStatement();
	sql::ResultSet* res = db.executeQuery(stmt, query.str());
	while (res->next())
	{
		Rule r(res);
		rules.insert(pair<int, Rule>(r.id, r));
	}
	delete res;
	delete stmt;
	return (int)rules.size();
}

void GeneratePoolStatistics(map<int, Team>& teams, DbConnectionDll::DbConnection& db)
{
	cout << "Generating Pool Statistics ..." << endl;
	map<int, Team>::iterator tit = teams.begin();
	while (tit != teams.end())
	{
		tit->second.GeneratePoolStatistics(db);
		tit++;
	}
}

void LogPoolStatistics(map<int, Pool>& pools, map<int, Team>& teams, ofstream& outf)
{
	cout << "Logging Pool Statistics ..." << endl;
	stringstream ss;
	outf << left << setw(40) << "Pool Name" << ",";
	map<int, Team>::iterator tit = teams.begin();
	while (tit != teams.end()) {
		if(tit->second.team_name.length() > 9)
			outf << left << setw(22) << tit->second.team_name << ",,";
		else {
			outf << right << setw(11) << tit->second.team_name << ",";
			outf << setw(11) << "" << ",";
		}
		tit++;
	}
	outf << right << setw(11) << "Total" << ",";
	outf << setw(11) << "" << endl;

	outf << left << setw(40) << setfill('-') << "-" << ",";
	tit = teams.begin();
	while (tit != teams.end()) {
		outf << right << setw(11) << "-" << "," << setw(11) << "-" << ",";
		tit++;
	}
	outf << right << setw(11) << "-" << "," << setw(11) << "-";
	outf << endl << setfill(' ');

	// Set up counters to tally totals
	map<int, int> nGTotalRecords;
	map<int, int> nGTotalDeals;
	map<int, int> nGTotalCallbacks;
	tit = teams.begin();
	while (tit != teams.end()) {
		nGTotalRecords.insert(pair<int, int>(tit->first, 0));
		nGTotalDeals.insert(pair<int, int>(tit->first, 0));
		nGTotalCallbacks.insert(pair<int, int>(tit->first, 0));
		tit++;
	}
	int nGGTotalDeals = 0;
	int nGGTotalCallbacks = 0;
	int nGGTotalRecords = 0;

	map<int, Pool>::iterator pit = pools.begin();
	while (pit != pools.end()) {
		int nTotalDeals = 0;
		int nTotalCallbacks = 0;
		int nTotalRecords = 0;

		ss.str("");
		ss << pit->second.name << " (" << pit->first << ")";
		outf << left << setw(40) << ss.str() << ",";
		tit = teams.begin();
		while (tit != teams.end())
		{
			ss.str("");
			map<int, int>::iterator rpit = tit->second.pools.find(pit->first);
			if (rpit != tit->second.pools.end()) {
				nGTotalRecords[tit->first] += rpit->second;
				nGGTotalRecords += rpit->second;
				nTotalRecords += rpit->second;
				ss << rpit->second;
			}
			else
				ss << 0;
			outf << right << setw(11) << ss.str() << ",";

			ss.str("");
			ss << " ";
			map<int, int>::iterator rdit = tit->second.deals.find(pit->first);
			if (rdit != tit->second.deals.end()) {
				nGTotalDeals[tit->first] += rdit->second;
				nGGTotalDeals += rdit->second;
				nTotalDeals += rdit->second;
				ss << "[" << rdit->second << "]";
			}
			map<int, int>::iterator rcit = tit->second.callbacks.find(pit->first);
			if (rcit != tit->second.callbacks.end()) {
				nGTotalCallbacks[tit->first] += rcit->second;
				nGGTotalCallbacks += rcit->second;
				nTotalCallbacks += rcit->second;
				ss << "{" << rcit->second << "}";
			}
			outf << left << setw(11) << ss.str() << ",";
			tit++;
		}

		outf << right << setw(11) << nTotalRecords << ",";
		ss.str("");
		ss << " ";
		if (nTotalDeals > 0)
			ss << "[" << nTotalDeals << "]";
		if (nTotalCallbacks > 0)
			ss << "{" << nTotalCallbacks << "}";
		outf << left << setw(11) << ss.str();
		outf << endl;
		pit++;
	}

	outf << left << setw(40) << "Total" << ",";
	tit = teams.begin();
	while (tit != teams.end())
	{
		outf << right << setw(11) << nGTotalRecords[tit->first] << ",";
		ss.str("");
		ss << " ";
		if (nGTotalDeals[tit->first] > 0) {
			ss << "[" << nGTotalDeals[tit->first] << "]";
		}
		if (nGTotalCallbacks[tit->first] > 0) {
			ss << "{" << nGTotalCallbacks[tit->first] << "}";
		}
		outf << left << setw(11) << ss.str() << ",";
		tit++;
	}

	outf << right << setw(11) << nGGTotalRecords << ",";
	ss.str("");
	ss << " ";
	if (nGGTotalDeals > 0)
		ss << "[" << nGGTotalDeals << "]";
	if (nGGTotalCallbacks > 0)
		ss << "{" << nGGTotalCallbacks << "}";
	outf << left << setw(11) << ss.str();
	outf << endl;

	outf << endl << " total [deals]{callbacks}" << endl << endl;
}

bool BackupDb(tm& now, ConnectionDetails& cd)
{
	string backupDir = FindDbBackupFolder();
	if (0 == backupDir.length())
		return false;

	stringstream backupCmd;
	backupCmd << "mysqldump -h" << cd.dbhost << " -u" << cd.dbuser << " -p" << cd.dbpwd << " " << cd.dbname << " > ";
	backupCmd << backupDir << "\\" << cd.dbname << "_";
	backupCmd << now.tm_year + 1900 << setfill('0') << setw(2) << now.tm_mon + 1 << setw(2) << now.tm_mday;
	backupCmd << "_";
	backupCmd << setw(2) << now.tm_hour << setw(2) << now.tm_min;
	backupCmd << ".bak";

	system(backupCmd.str().c_str());

	return true;
}

string FindDbBackupFolder()
{
	struct stat buffer;

	string backupDir = "C:\\Users\\rob\\Documents\\DbBackups";
	if (0 == stat(backupDir.c_str(), &buffer))
		return backupDir;

	backupDir = "C:\\DBbackups";
	if (0 == stat(backupDir.c_str(), &buffer))
		return backupDir;

	backupDir = "D:\\UtilityPower\\fineIT\\DBbackups";
	if (0 == stat(backupDir.c_str(), &buffer))
		return backupDir;

	backupDir = "";
	return backupDir;
}

void help(char* argv[]) {
	cout << UtilitiesDll::Utilities::GetFileNameWithoutExtension(argv[0]) << endl;
	cout << "[" << endl;
	cout << "  /? : Show help |" << endl;
	cout << "  /RECREATESEGMENTATIONRULES : Recreate the segmentation_rules table |" << endl;
	cout << "  /RECREATETEAMS : Recreate the teams table |" << endl;
	cout << "  /TOPUP : Properly do the top up" << endl;
	cout << "  /COUNTS : Just write the stats report" << endl;
	cout << "  /ASSIGNONLY : Just free and reassign using the existing working table" << endl;
	cout << "  /CORP : Corporate data" << endl;
	cout << "  /V : Show the version number" << endl;
	cout << "]" << endl;
}



