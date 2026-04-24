#include <bits/stdc++.h>
using namespace std;
#define int long long

signed main() {
    ios::sync_with_stdio(false);
    cin.tie(NULL);

    string line, current_key = "";
    int total_bytes = 0;
    int request_count = 0;
    unordered_set<string> hosts;

    while(getline(cin, line)) {
        int tab = line.find('\t');
        string key = line.substr(0, tab);
        string val = line.substr(tab + 1);

        if(current_key != "" && key != current_key) {
            cout << current_key << "\t"
                 << request_count << "\t"
                 << total_bytes << "\t"
                 << hosts.size() << "\n";

            total_bytes = 0;
            request_count = 0;
            hosts.clear();
        }

        current_key = key;

        if(val[0] == 'B') {
            int bytes = stoll(val.substr(2));
            total_bytes += bytes;
            request_count++;
        } else if(val[0] == 'H') {
            hosts.insert(val.substr(2));
        }
    }

    if(current_key != "") {
        cout << current_key << "\t"
             << request_count << "\t"
             << total_bytes << "\t"
             << hosts.size() << "\n";
    }

    return 0;
}