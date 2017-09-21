#pragma once

#include <iostream>
#include <map>
#include <vector>

#include "C:/Users/Rob/Documents/Visual Studio 2015/Projects/Utilities/Utilities/Utilities.h"
#include "C:/Users/Rob/Documents/Visual Studio 2015/Projects/Formatter/Formatter/Formatter.h"
#include "C:/Users/Rob/Documents/Visual Studio 2015/Projects/DbConnection/DbConnection/DbConnection.h"
#include "Rule.h"
#include "Pool.h"
#include "Distributor.h"

using namespace std;

class WorkingTable
{
public:
	WorkingTable();
	~WorkingTable();

	static void CreateTable(DbConnectionDll::DbConnection& db);
	static void Populate(DbConnectionDll::DbConnection& db);
	static void BuildIndexes(DbConnectionDll::DbConnection& db);
	static void BuildIndex(string idx_name, string tbl_name, DbConnectionDll::DbConnection& db);
	static void Segment(map<int, Rule>& rules, DbConnectionDll::DbConnection& db, bool bCorp);
	static void FreeNonMatchingSites(map<int, Team>& teams, DbConnectionDll::DbConnection& db);
	static void FreeNonMatchingSite(int site_id, DbConnectionDll::DbConnection& db);
	static void FreeNonMatchingGroup(int group_id, DbConnectionDll::DbConnection& db);
	static void AssignSites(map<int, Team>& teams, map<int, Pool>& pools, DbConnectionDll::DbConnection& db);
	static void AssignSitesToTeam(map<int, Team>::iterator team, map<int, Pool>& pools, DbConnectionDll::DbConnection& db);
	static void AssignSite(int site_id, Pool& pool, DbConnectionDll::DbConnection& db);
	static void AssignGroup(int group_id, Pool& pool, DbConnectionDll::DbConnection& db);
	static void DropTable(DbConnectionDll::DbConnection& db);
	static void SetPoolWeightsForTeam(map<int, Team>::iterator team, map<int, Pool>& pools);
	static void ApplyChanges(DbConnectionDll::DbConnection&db);
	static void ApplySiteChanges(DbConnectionDll::DbConnection&db);
	static void ApplyGroupChanges(DbConnectionDll::DbConnection&db);
	static void UpdateCallbacks(DbConnectionDll::DbConnection&db);
	static void RemoveUserIdsFromCallbacks(DbConnectionDll::DbConnection&db);
	static void ApplySiteCallbackChanges(DbConnectionDll::DbConnection&db);
	static void ApplyGroupCallbackChanges(DbConnectionDll::DbConnection&db);
	static void DeleteOrphanedCallbacks(DbConnectionDll::DbConnection&db);

	// Helper Functions
	static void SetDaysLeft(DbConnectionDll::DbConnection& db);
	static void SetCallbackFlag(DbConnectionDll::DbConnection&db);
	static void FindDealStatus(DbConnectionDll::DbConnection&db);
	static void FindSupplier(DbConnectionDll::DbConnection&db);
	static void FlagHalfHourlies(DbConnectionDll::DbConnection&db);
	static void FlagMaxDemands(DbConnectionDll::DbConnection&db);
	static void FlagRelatedMeters(DbConnectionDll::DbConnection&db);
	static void FlagRefreshConfirmedMeters(DbConnectionDll::DbConnection&db, string wtFieldName, string refresh_date);
	static void FlagFlexibleTypeContracts(DbConnectionDll::DbConnection&db, string contract_type);
	static void CalculateConsumption(DbConnectionDll::DbConnection&db);
	static void FlagDeEnergisedMeters(DbConnectionDll::DbConnection&db);
	static void FindSource(DbConnectionDll::DbConnection&db);
	static void CountNotes(DbConnectionDll::DbConnection& db);
};

