#include "Distributor.h"

Distributor::Distributor()
{
}

Distributor::~Distributor()
{
}

Distributor::Distributor(map<int, Team>::iterator team, int _pool_id, int _max_count) {
	pool_id = _pool_id;
	max_count = _max_count;
	current_count = team->second.pools[pool_id] - team->second.callbacks[pool_id] - team->second.deals[pool_id];
}

