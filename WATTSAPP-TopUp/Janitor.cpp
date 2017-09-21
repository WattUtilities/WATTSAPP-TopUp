#include "Janitor.h"



Janitor::Janitor()
{
}


Janitor::~Janitor()
{
}

void Janitor::PerformRegularMaintenance(DbConnectionDll::DbConnection & db)
{
	UpdateNextRenewalDate(db);
	ReleaseViewLocks(db);
	UpdateSourceOfWebsiteLeads(db);
	TakeCareOfCeasedSites(db);
	TakeCareOfNightData(db);
	ReleaseLeaversData(db);
}

void Janitor::UpdateNextRenewalDate(DbConnectionDll::DbConnection & db)
{
	cout << "Updating Next Renewal Date ..." << endl;
	sql::Statement* stmt = db.createStatement();
	stringstream query;

	query << "SET @@global.sql_mode = ''";
	db.execute(stmt, query.str());

	query.str("");
	query << "UPDATE sites s";
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
	query << " ON s.id = sq.site_id";
	query << " SET s.next_renewal_date = sq.next_renewal_date";
	db.executeUpdate(stmt, query.str());

	query.str("");
	query << "UPDATE sites s";
	query << " SET s.next_renewal_date = '0000-00-00'";
	query << " WHERE s.next_renewal_date IS NULL";
	db.executeUpdate(stmt, query.str());

	delete stmt;
}

void Janitor::ReleaseViewLocks(DbConnectionDll::DbConnection & db)
{
	cout << "Releasing View Locks ..." << endl;
	stringstream query;
	query << "UPDATE sites s";
	query << " SET locked = 0";
	sql::Statement* stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());

	delete stmt;
}

void Janitor::UpdateSourceOfWebsiteLeads(DbConnectionDll::DbConnection & db)
{
	cout << "Updating Source of Website Leads ..." << endl;
	stringstream query;
	query << "UPDATE sites s";
	query << " SET source = 1,";
	query << " grouping_id = 0";
	query << " WHERE web_customer_id LIKE 'WATT%'";
	sql::Statement* stmt = db.createStatement();
	db.executeUpdate(stmt, query.str());

	query.str("");
	query << "UPDATE sites s";
	query << " SET source = 1,";
	query << " grouping_id = 0";
	query << " WHERE web_customer_id REGEXP '^-?[0-9]+$'"; // catches all the purely numeric web_customer_ids
	db.executeUpdate(stmt, query.str());

	query.str("");
	query << "UPDATE sites s";
	query << " SET source = 2,";
	query << " grouping_id = 0";
	query << " WHERE web_customer_id LIKE 'BCWATT%'";
	db.executeUpdate(stmt, query.str());

	query.str("");
	query << "UPDATE sites s";
	query << " SET source = 3,";
	query << " grouping_id = 0";
	query << " WHERE web_customer_id LIKE 'DPWATT%'";
	db.executeUpdate(stmt, query.str());

	delete stmt;
}

void Janitor::TakeCareOfCeasedSites(DbConnectionDll::DbConnection& db)
{
	cout << "Taking Care of Ceased Sites ..." << endl;
	stringstream query;
	sql::Statement* stmt = db.createStatement();

	/**
	Insert into the ceased table sites that have recently been marked as ceased but aren't yet in the ceased pool
	**/
	query << "INSERT INTO ceased";
	query << " SELECT null, u.id, s.id, null, null";
	query << " FROM sites s";
	query << " LEFT JOIN users u ON s.pool_id = u.primary_pool_id";
	query << " WHERE s.ceased > 0";
	query << " AND s.pool_id != 1001";
	db.execute(stmt, query.str());

	/**
	Move all ceased sites into the ceased pool
	**/
	query.str("");
	query << "UPDATE sites SET pool_id = 1001 WHERE ceased > 0";
	db.execute(stmt, query.str());

	/**
	Mark all records in the ceased pool as ceased
	**/
	query.str("");
	query << "UPDATE sites SET ceased = TRUE WHERE pool_id = 1001";
	db.execute(stmt, query.str());

	delete stmt;
}

void Janitor::TakeCareOfNightData(DbConnectionDll::DbConnection& db)
{
	cout << "Taking Care of Night Data ..." << endl;
	stringstream query;
	sql::Statement* stmt = db.createStatement();

	/**
	Move all night_data sites into the night_data pool
	**/
	query << "UPDATE sites SET pool_id = 1002 WHERE night_data > 0";
	db.execute(stmt, query.str());

	/**
	Mark all records in the night_data pool as night_data
	**/
	query.str("");
	query << "UPDATE sites SET night_data = TRUE WHERE pool_id = 1002";
	db.execute(stmt, query.str());
	query.str("");

	delete stmt;
}

void Janitor::ReleaseLeaversData(DbConnectionDll::DbConnection& db)
{
	cout << "Freeing Leavers Data ..." << endl;
	stringstream query;
	sql::Statement* stmt = db.createStatement();

	/**
	Throw leaver's sites back into the cloud
	**/
	query << "UPDATE sites s";
	query << " LEFT JOIN users u ON u.primary_pool_id = s.pool_id";
	query << " SET s.pool_id = 0";
	query << " WHERE u.status = 2";
	db.executeUpdate(stmt, query.str());

	/**
	Remove leaver's callbacks
	**/
	query.str("");
	query << "DELETE cb";
	query << " FROM callbacks cb";
	query << " LEFT JOIN users u ON u.id = cb.user_id";
	query << " WHERE u.status = 2";
	db.execute(stmt, query.str());
	query.str("");

	/**
	Remove leaver's team associations
	**/
	query.str("");
	query << "DELETE pt";
	query << " FROM pool_team pt";
	query << " LEFT JOIN users u ON u.primary_pool_id = pt.pool_id";
	query << " WHERE u.status = 2";
	db.execute(stmt, query.str());
	query.str("");

	/**
	Remove leaver's non-primary pool associations
	**/
	query.str("");
	query << "DELETE pu";
	query << " FROM pool_user pu";
	query << " LEFT JOIN users u ON u.id = pu.user_id";
	query << " WHERE u.status = 2";
	query << " AND u.primary_pool_id != pu.pool_id";
		db.execute(stmt, query.str());
	query.str("");

	delete stmt;
}




