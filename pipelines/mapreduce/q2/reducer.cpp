#include <bits/stdc++.h>
using namespace std;
#define int long long

signed main() {
    ios::sync_with_stdio(false);
    cin.tie(NULL);

    string line;
    string current_key = "";

    int total_count = 0;
    int total_bytes = 0;
    unordered_set<string> hosts;

    while(getline(cin, line)) {
        vector<string> parts;
        string temp;
        stringstream ss(line);

        while(getline(ss, temp, '\t')) {
            parts.push_back(temp);
        }

        string key = parts[0];

        int count = 0, bytes = 0;

        // CASE 1: mapper output → bytes|host
        if(parts.size() == 2) {
            int sep = parts[1].find('|');
            bytes = stoll(parts[1].substr(0, sep));
            string host = parts[1].substr(sep + 1);

            count = 1;
            hosts.insert(host);
        }
        // CASE 2: reducer output → count, bytes, distinct_hosts
        else {
            count = stoll(parts[1]);
            bytes = stoll(parts[2]);

            // we cannot reconstruct exact hosts, so approximate
            int distinct_hosts = stoll(parts[3]);

            total_count += count;
            total_bytes += bytes;

            // approximate distinct hosts (not exact merge)
            for(int i = 0; i < distinct_hosts; i++) {
                hosts.insert(to_string(i) + key);
            }

            continue;
        }

        if(key != current_key && current_key != "") {
            cout << current_key << "\t"
                 << total_count << "\t"
                 << total_bytes << "\t"
                 << hosts.size() << "\n";

            total_count = 0;
            total_bytes = 0;
            hosts.clear();
        }

        current_key = key;
        total_count += count;
        total_bytes += bytes;
    }

    if(current_key != "") {
        cout << current_key << "\t"
             << total_count << "\t"
             << total_bytes << "\t"
             << hosts.size() << "\n";
    }

    return 0;
}
