#include "TableSafe.h"



TableSafe::TableSafe()
{
}


TableSafe::~TableSafe()
{
}

void TableSafe::RecreateSegmentationRules(DbConnectionDll::DbConnection& db)
{
	cout << "Recreacting segmentation_rules ..." << endl;
	stringstream query;
	sql::Statement* stmt = db.createStatement();

	query << "DROP TABLE IF EXISTS segmentation_rules";
	db.execute(stmt, query.str());

	query.str("");
	query << "CREATE TABLE segmentation_rules(";
	query << " id int unsigned not null default 0 primary key,";
	query << " team_id int unsigned not null default 0,";
	query << " description varchar(255),";
	query << " query varchar(255) not null default '' ";
	query << ")";
	db.execute(stmt, query.str());

	query.str("");
	query << "INSERT INTO segmentation_rules VALUES(";
	query << " 1,";
	query << " 11,";
	query << " 'Ceased sites',";
	query << " 'isCeased' ";
	query << ")";
	db.execute(stmt, query.str());

	query.str("");
	query << "INSERT INTO segmentation_rules VALUES(";
	query << " 2,";
	query << " 12,";
	query << " 'Night data',";
	query << " 'isNightData' ";
	query << ")";
	db.execute(stmt, query.str());

	query.str("");
	query << "INSERT INTO segmentation_rules VALUES(";
	query << " 3,";
	query << " 10,";
	query << " 'Web leads < 180',";
	query << " 'source = 1 AND days_left <= 180' ";
	query << ")";
	db.execute(stmt, query.str());

	query.str("");
	query << "INSERT INTO segmentation_rules VALUES(";
	query << " 4,";
	query << " 1,";
	query << " 'Half hourlies < 180',";
	query << " 'isHalfHourly AND days_left <= 180' ";
	query << ")";
	db.execute(stmt, query.str());

	query.str("");
	query << "INSERT INTO segmentation_rules VALUES(";
	query << " 5,";
	query << " 2,";
	query << " 'Supplier transfer < 180',";
	query << " 'supplier = 0 AND days_left <= 180' ";
	query << ")";
	db.execute(stmt, query.str());

	query.str("");
	query << "INSERT INTO segmentation_rules VALUES(";
	query << " 6,";
	query << " 3,";
	query << " 'Eon < 180',";
	query << " 'supplier = 2 AND days_left <= 180' ";
	query << ")";
	db.execute(stmt, query.str());

	query.str("");
	query << "INSERT INTO segmentation_rules VALUES(";
	query << " 7,";
	query << " 2,";
	query << " 'Npower < 180 but not confirmed by refresh => Supplier Transfers',";
	query << " '!isConfirmedByLastRefresh AND days_left <= 180' ";
	query << ")";
	db.execute(stmt, query.str());

	query.str("");
	query << "INSERT INTO segmentation_rules VALUES(";
	query << " 8,";
	query << " 4,";
	query << " 'Npower related meters < 180',";
	query << " 'isRelated AND days_left <= 180' ";
	query << ")";
	db.execute(stmt, query.str());

	query.str("");
	query << "INSERT INTO segmentation_rules VALUES(";
	query << " 9,";
	query << " 5,";
	query << " 'Npower max demands < 180',";
	query << " 'isMaxDemand AND days_left <= 180' ";
	query << ")";
	db.execute(stmt, query.str());

	query.str("");
	query << "INSERT INTO segmentation_rules VALUES(";
	query << " 10,";
	query << " 9,";
	query << " 'Npower < 5000 eac & flexible',";
	query << " 'eac < 5000 AND isFlexibleEtc' ";
	query << ")";
	db.execute(stmt, query.str());

	query.str("");
	query << "INSERT INTO segmentation_rules VALUES(";
	query << " 11,";
	query << " 9,";
	query << " 'Npower < 5000 eac < 180',";
	query << " 'eac < 5000 AND days_left <= 180' ";
	query << ")";
	db.execute(stmt, query.str());

	query.str("");
	query << "INSERT INTO segmentation_rules VALUES(";
	query << " 12,";
	query << " 13,";
	query << " 'Npower flexible contracts',";
	query << " 'isFlexibleEtc' ";
	query << ")";
	db.execute(stmt, query.str());

	query.str("");
	query << "INSERT INTO segmentation_rules VALUES(";
	query << " 13,";
	query << " 6,";
	query << " 'Npower reds',";
	query << " 'days_left < 0' ";
	query << ")";
	db.execute(stmt, query.str());

	query.str("");
	query << "INSERT INTO segmentation_rules VALUES(";
	query << " 14,";
	query << " 7,";
	query << " 'Npower yellows',";
	query << " 'days_left <= 90' ";
	query << ")";
	db.execute(stmt, query.str());

	query.str("");
	query << "INSERT INTO segmentation_rules VALUES(";
	query << " 15,";
	query << " 8,";
	query << " 'Npower greens',";
	query << " 'days_left <= 180' ";
	query << ")";
	db.execute(stmt, query.str());

	delete stmt;
}

void TableSafe::RecreateTeams(DbConnectionDll::DbConnection& db)
{
	cout << "Recreacting teams ..." << endl;
	stringstream query;
	sql::Statement* stmt = db.createStatement();

	query << "DROP TABLE IF EXISTS teams";
	db.execute(stmt, query.str());

	query.str("");
	query << "CREATE TABLE teams(";
	query << " id int unsigned not null default 0 primary key,";
	query << " weight int not null default 0,";
	query << " team_name varchar(255) ";
	query << ")";
	db.execute(stmt, query.str());

	query.str("");
	query << "INSERT INTO teams VALUES(";
	query << " 1,";
	query << " 0,";
	query << " 'Half Hourlies' ";
	query << ")";
	db.execute(stmt, query.str());

	query.str("");
	query << "INSERT INTO teams VALUES(";
	query << " 2,";
	query << " 1,";
	query << " 'Supplier Transfers' ";
	query << ")";
	db.execute(stmt, query.str());

	query.str("");
	query << "INSERT INTO teams VALUES(";
	query << " 3,";
	query << " 1,";
	query << " 'Eon' ";
	query << ")";
	db.execute(stmt, query.str());

	query.str("");
	query << "INSERT INTO teams VALUES(";
	query << " 4,";
	query << " 1,";
	query << " 'Related Meters' ";
	query << ")";
	db.execute(stmt, query.str());

	query.str("");
	query << "INSERT INTO teams VALUES(";
	query << " 5,";
	query << " 1,";
	query << " 'Max Demands' ";
	query << ")";
	db.execute(stmt, query.str());

	query.str("");
	query << "INSERT INTO teams VALUES(";
	query << " 6,";
	query << " 1,";
	query << " 'Reds' ";
	query << ")";
	db.execute(stmt, query.str());

	query.str("");
	query << "INSERT INTO teams VALUES(";
	query << " 7,";
	query << " 1,";
	query << " 'Yellows' ";
	query << ")";
	db.execute(stmt, query.str());

	query.str("");
	query << "INSERT INTO teams VALUES(";
	query << " 8,";
	query << " 1,";
	query << " 'Greens' ";
	query << ")";
	db.execute(stmt, query.str());

	query.str("");
	query << "INSERT INTO teams VALUES(";
	query << " 9,";
	query << " 1,";
	query << " 'Trainee' ";
	query << ")";
	db.execute(stmt, query.str());

	query.str("");
	query << "INSERT INTO teams VALUES(";
	query << " 10,";
	query << " 1,";
	query << " 'Inbound / Web' ";
	query << ")";
	db.execute(stmt, query.str());

	query.str("");
	query << "INSERT INTO teams VALUES(";
	query << " 11,";
	query << " 1,";
	query << " 'Ceased' ";
	query << ")";
	db.execute(stmt, query.str());

	query.str("");
	query << "INSERT INTO teams VALUES(";
	query << " 12,";
	query << " 1,";
	query << " 'Night Data' ";
	query << ")";
	db.execute(stmt, query.str());

	query.str("");
	query << "INSERT INTO teams VALUES(";
	query << " 13,";
	query << " 1,";
	query << " 'Flexible' ";
	query << ")";
	db.execute(stmt, query.str());

	delete stmt;
}

