import collections

def reduce_q1(input_file, output_file):
    agg = collections.defaultdict(lambda: [0, 0])

    with open(input_file) as f:
        for line in f:
            key, v1, v2, _ = line.strip().split("\t")

            date, status = key.split("|")
            agg[key][0] += int(v1)
            agg[key][1] += int(v2)

    with open(output_file, "w") as f:
        for key, (cnt, byt) in agg.items():
            f.write(f"{key}\t{cnt}\t{byt}\t0\n")

def reduce_q2(input_file, output_file):
    agg = collections.defaultdict(lambda: [0, 0, 0])

    with open(input_file) as f:
        for line in f:
            key, v1, v2, v3 = line.strip().split("\t")

            agg[key][0] += int(v1)
            agg[key][1] += int(v2)
            agg[key][2] += int(v3)

    with open(output_file, "w") as f:
        for key, (cnt, byt, hosts) in agg.items():
            f.write(f"{key}\t{cnt}\t{byt}\t{hosts}\n")


def reduce_q3(input_file, output_file):
    agg = collections.defaultdict(lambda: [0, 0])

    with open(input_file) as f:
        for line in f:
            key, v1, v2, v3 = line.strip().split("\t")

            agg[key][0] += int(v1)
            agg[key][1] += int(v2)

    with open(output_file, "w") as f:
        for key, (err, total) in agg.items():
            rate = err / total if total != 0 else 0
            f.write(f"{key}\t{err}\t{total}\t{rate}\n")