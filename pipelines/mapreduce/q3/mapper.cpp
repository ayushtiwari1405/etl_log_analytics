#include <bits/stdc++.h>
using namespace std;
#define int long long

signed main() {
    ios::sync_with_stdio(false);
    cin.tie(NULL);

    string line;
    regex pattern(R"(^(\S+) \S+ \S+ \[([^\]]+)\] \"([^\"]*)\" (\d{3}) (\S+))");

    int malformed = 0;

    unordered_map<string,string> month_map = {
        {"Jan","01"},{"Feb","02"},{"Mar","03"},{"Apr","04"},
        {"May","05"},{"Jun","06"},{"Jul","07"},{"Aug","08"},
        {"Sep","09"},{"Oct","10"},{"Nov","11"},{"Dec","12"}
    };

    while(getline(cin, line)) {
        smatch match;

        if(!regex_search(line, match, pattern)) {
            malformed++;
            continue;
        }

        string host = match[1];
        string timestamp = match[2];

        // safety check for timestamp length
        if(timestamp.size() < 14) {
            malformed++;
            continue;
        }

        int status;
        try {
            status = stoll(match[4]);
        } catch(...) {
            malformed++;
            continue;
        }

        string date = timestamp.substr(0, 11);
        string hour = timestamp.substr(12, 2);

        string dd = date.substr(0,2);
        string mon = date.substr(3,3);
        string yyyy = date.substr(7,4);

        if(month_map.find(mon) == month_map.end()) {
            malformed++;
            continue;
        }

        string log_date = yyyy + "-" + month_map[mon] + "-" + dd;

        int is_error = (status >= 400 && status <= 599) ? 1 : 0;

        cout << log_date << "|" << hour << "\t"
             << is_error << "|" << host << "\n";
    }

    cerr << "Malformed Count: " << malformed << "\n";

    return 0;
}