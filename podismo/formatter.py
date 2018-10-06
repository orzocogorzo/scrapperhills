# 0.riferimento 1.luogo 2.giorno 3.data 4.nome 5.km 6.telefoni
# 7.note 8.email 9.url 10.date 11.title 12.mail 13.web 14.url 15.status

import csv
import re

file = open('podismo.csv')

podismo = [row for row in csv.reader(file)]
titles = podismo[0]
podismo = [row for row in podismo if row[0] != "riferimento"]
file.close()

file = open('podismo_definitivo.csv', 'w')
writer = csv.writer(file)
writer.writerow(titles)
for row in podismo:
    writer.writerow(row)

file.close()

file = open('podismo_definitivo.csv', 'r')
podismo = [row for row in csv.reader(file)]
file.close()

for i in range(len(podismo)):
    new_row = [cell for cell in podismo[i] if cell != ""]
    podismo[i] = new_row
    for ci in range(len(podismo[i])):
        # move urls to 9th col
        if ci >= len(podismo[i]):
            pass
        elif "calendariopodismo" in podismo[i][ci]:
            new_row = [cell for cell in list(podismo[i]) if "calendariopodismo" not in cell] + [podismo[i][ci]]
            podismo[i] = new_row

for i in range(len(podismo)):
    for ci in range(len(podismo[i])):
        # move emails to 8th col
        if ci >= len(podismo[i]):
            pass
        elif "@" in podismo[i][ci]:
            new_row = [cell for cell in list(podismo[i]) if "@" not in cell] + [podismo[i][ci]]
            podismo[i] = new_row


for i in range(len(podismo)):
    for ci in range(len(podismo[i])):
        # move webs to the 13th column
        if ci >= len(podismo[i]):
            pass
        elif ("www" in podismo[i][ci] or "http" in podismo[i][ci]) and "calendariopodismo" not in podismo[i][ci]:
            new_row = [cell for cell in podismo[i]
                       if ("www" in podismo[i][ci] or "http" in podismo[i][ci])
                       and "calendariopodismo" not in podismo[i][ci]] + [podismo[i][ci]]
            podismo[i] = new_row

file = open('podismo_definitivo.csv', 'w')
writer = csv.writer(file)
for row in podismo:
    writer.writerow(row)

file.close()

file = open('podismo_definitivo.csv','r')
podismo = [row for row in csv.reader(file)]
file.close()

titles = ['riferimento', 'luogo', 'giorno', 'data', 'nome', 'km', 'telefoni', 'note', 'email', 'web', 'url']
p = re.compile("\w{4,10}\-?")
new_matrix = list()
for i in range(len(podismo))[1:]:
    new_row = list(podismo[i][:6])
    tel = podismo[i][6]
    if tel != "" and p.search(tel) != None:
        new_row.append(podismo[i][6])
    else:
        for ci in range(len(podismo[i])):
            # search tel
            m = p.search(podismo[i][ci])
            if m:
                new_row.append(podismo[i][ci])
                break

    new_row.append(podismo[i][7])

    for ci in range(len(podismo[i])):
        # search email
        if "@" in podismo[i][ci]:
            new_row.append(podismo[i][ci])
            break
        elif ci == len(podismo[i])-1:
            new_row.append("")

    for ci in range(len(podismo[i])):
        # search web
        if ("www" in podismo[i][ci] or "http" in podismo[i][ci]) and "calendariopodismo" not in podismo[i][ci]:
            new_row.append(podismo[i][ci])
            break
        elif ci == len(podismo[i])-1:
            new_row.append("")

    for ci in range(len(podismo[i])):
        # search web
        if "calendariopodismo" in podismo[i][ci]:
            new_row.append(podismo[i][ci])
            break
        elif ci == len(podismo[i])-1:
            new_row.append("")

    new_matrix.append(new_row)

file = open('podismo_definitivo.csv','w')
writer = csv.writer(file)
writer.writerow(titles)
for row in new_matrix:
    writer.writerow(row)