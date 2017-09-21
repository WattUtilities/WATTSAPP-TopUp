#pragma once
#include "Team.h"

class Distributor
{
public:
	Distributor();
	~Distributor();
	Distributor(map<int, Team>::iterator team, int _pool_id, int _max_count);

	int pool_id;
	int max_count;
	int current_count;
};

