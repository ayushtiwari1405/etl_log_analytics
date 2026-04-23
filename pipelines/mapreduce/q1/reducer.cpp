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

    while(getline(cin, line)) {
        vector<string> parts;
        string temp;
        stringstream ss(line);

        while(getline(ss, temp, '\t')) {
            parts.push_back(temp);
        }

        string key = parts[0];

        int count, bytes;

        // Case 1: raw mapper output (value only)
        if(parts.size() == 2) {
            count = 1;
            bytes = stoll(parts[1]);
        }
        // Case 2: intermediate reducer output
        else {
            count = stoll(parts[1]);
            bytes = stoll(parts[2]);
        }

        if(key != current_key && current_key != "") {
            cout << current_key << "\t" << total_count << "\t" << total_bytes << "\n";
            total_count = 0;
            total_bytes = 0;
        }

        current_key = key;
        total_count += count;
        total_bytes += bytes;
    }

    if(current_key != "") {
        cout << current_key << "\t" << total_count << "\t" << total_bytes << "\n";
    }

    return 0;
}
