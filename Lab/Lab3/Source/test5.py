existing = []
dict = {}
with open("C:\\Users\Sirisha Sunkara\Desktop\Summer18\Spark\\7-11\Pyspark\Pyspark\\facebook_combined.txt") as fp:
    for l in fp:
        l = l.split(" ")
        u = l[0]
        f = l[1]
        if u in existing:
            dict[u] = dict[u] + ',' + f
        else:
            existing.append(u)
            dict[u] = f

f = open('sample2.txt', 'w')
for k in dict:
    t = k + '->' + dict[k].replace('\n','')+'\n'
    f.write(t)
