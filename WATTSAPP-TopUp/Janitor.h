#pragma once

#include <iostream>

#include "C:/Users/Rob/Documents/Visual Studio 2015/Projects/Utilities/Utilities/Utilities.h"
#include "C:/Users/Rob/Documents/Visual Studio 2015/Projects/Formatter/Formatter/Formatter.h"
#include "C:/Users/Rob/Documents/Visual Studio 2015/Projects/DbConnection/DbConnection/DbConnection.h"

class Janitor
{
public:
	Janitor();
	~Janitor();

	static void PerformRegularMaintenance(DbConnectionDll::DbConnection& db);
	static void UpdateNextRenewalDate(DbConnectionDll::DbConnection& db);
	static void ReleaseViewLocks(DbConnectionDll::DbConnection& db);
	static void UpdateSourceOfWebsiteLeads(DbConnectionDll::DbConnection& db);
	static void TakeCareOfCeasedSites(DbConnectionDll::DbConnection& db);
	static void TakeCareOfNightData(DbConnectionDll::DbConnection& db);
	static void ReleaseLeaversData(DbConnectionDll::DbConnection& db);
};

