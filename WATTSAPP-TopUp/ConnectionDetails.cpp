#include "ConnectionDetails.h"



ConnectionDetails::ConnectionDetails()
{
	Init();
}

void ConnectionDetails::Init()
{
	dbhost = "localhost";
	dbuser = "root";
	dbpwd = "makaton";
	dbname = "watts_app";
}


ConnectionDetails::~ConnectionDetails()
{
}

ConnectionDetails::ConnectionDetails(int argc, char* argv[])
{
	Init();
	for (int i = 1; i < argc; i++) {
		string s = argv[i];
		string sw = s.substr(0, 2);
		if (sw == "-h")
			dbhost = s.substr(2);
		if (sw == "-u")
			dbuser = s.substr(2);
		if (sw == "-p")
			dbpwd = s.substr(2);
		if (sw == "-n")
			dbname = s.substr(2);
	}
}

