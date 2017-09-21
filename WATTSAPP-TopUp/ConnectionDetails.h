#pragma once

#include <string>

using namespace std;

class ConnectionDetails
{
public:
	ConnectionDetails();
	~ConnectionDetails();
	ConnectionDetails(int argc, char* argv[]);
	void Init();

	string dbhost;
	string dbuser;
	string dbpwd;
	string dbname;
};

