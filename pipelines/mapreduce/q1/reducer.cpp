#include <bits/stdc++.h>
using namespace std;
#define int long long

signed main() {
    ios::sync_with_stdio(false);
    cin.tie(NULL);

    string line, current_key = "";
    int total_bytes = 0;
    int request_count = 0;

    while(getline(cin, line)) {
        int tab = line.find('\t');
        string key = line.substr(0, tab);
        int bytes = stoll(line.substr(tab + 1));

        if(current_key != "" && key != current_key) {
            cout << current_key << "\t"
                 << request_count << "\t"
                 << total_bytes << "\t0\n";

            request_count = 0;
            total_bytes = 0;
        }

        current_key = key;
        request_count++;
        total_bytes += bytes;
    }

    if(current_key != "") {
        cout << current_key << "\t"
             << request_count << "\t"
             << total_bytes << "\t0\n";
    }

    return 0;
}