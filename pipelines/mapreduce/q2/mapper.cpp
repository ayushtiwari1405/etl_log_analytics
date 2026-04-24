#include <bits/stdc++.h>
using namespace std;
#define int long long

signed main() {
    ios::sync_with_stdio(false);
    cin.tie(NULL);

    string line;
    regex pattern(R"(^(\S+) \S+ \S+ \[([^\]]+)\] \"([^\"]*)\" (\d{3}) (\S+))");

    int malformed = 0;

    while(getline(cin, line)) {
        smatch match;

        if(!regex_search(line, match, pattern)) {
            malformed++;
            continue;
        }

        string host = match[1];
        string request = match[3];
        string bytes_str = match[5];

        int bytes = (bytes_str == "-") ? 0 : stoll(bytes_str);

        stringstream ss(request);
        vector<string> parts;
        string temp;

        while(ss >> temp) parts.push_back(temp);

        if(parts.size() < 2) {
            malformed++;
            continue;
        }

        string resource = parts[1];

        // bytes stream
        cout << resource << "\tB|" << bytes << "\n";

        // host stream
        cout << resource << "\tH|" << host << "\n";
    }

    cerr << "Malformed Count: " << malformed << "\n";

    return 0;
}