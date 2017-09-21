#pragma once

#include "C:/Users/Rob/Documents/Visual Studio 2015/Projects/DbConnection/DbConnection/DbConnection.h"

class TableSafe
{
public:
	TableSafe();
	~TableSafe();

	static void RecreateSegmentationRules(DbConnectionDll::DbConnection& db);
	static void RecreateTeams(DbConnectionDll::DbConnection& db);
};

