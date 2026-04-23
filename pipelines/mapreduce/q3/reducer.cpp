#include <bits/stdc++.h>
using namespace std;
#define int long long

signed main() {
    ios::sync_with_stdio(false);
    cin.tie(NULL);

    string line;
    string current_key = "";

    int total_requests = 0;
    int error_requests = 0;
    unordered_set<string> error_hosts;

    while(getline(cin, line)) {
        vector<string> parts;
        string temp;
        stringstream ss(line);

        while(getline(ss, temp, '\t')) {
            parts.push_back(temp);
        }

        string key = parts[0];

        // CASE 1: mapper output → is_error|host
        if(parts.size() == 2) {
            int sep = parts[1].find('|');
            int is_error = stoll(parts[1].substr(0, sep));
            string host = parts[1].substr(sep + 1);

            total_requests++;

            if(is_error) {
                error_requests++;
                error_hosts.insert(host);
            }
        }
        // CASE 2: reducer output → error_count, total_count, error_rate, hosts
        else {
            int err = stoll(parts[1]);
            int tot = stoll(parts[2]);
            int hosts = stoll(parts[4]);

            total_requests += tot;
            error_requests += err;

            for(int i = 0; i < hosts; i++) {
                error_hosts.insert(to_string(i) + key);
            }

            continue;
        }

        if(key != current_key && current_key != "") {
            double rate = (double)error_requests / total_requests;

            cout << current_key << "\t"
                 << error_requests << "\t"
                 << total_requests << "\t"
                 << rate << "\t"
                 << error_hosts.size() << "\n";

            total_requests = 0;
            error_requests = 0;
            error_hosts.clear();
        }

        current_key = key;
    }

    if(current_key != "") {
        double rate = (double)error_requests / total_requests;

        cout << current_key << "\t"
             << error_requests << "\t"
             << total_requests << "\t"
             << rate << "\t"
             << error_hosts.size() << "\n";
    }

    return 0;
}
