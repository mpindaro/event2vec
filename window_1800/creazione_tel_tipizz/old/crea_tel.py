import csv
import os
import calendar
import time

fonte = "telefoni.csv"
sorgente = open(fonte)
reader = csv.reader(sorgente, delimiter=",")

telefoni = {}
celle = {}

for rec in reader:
    telefoni[rec[1]] = [rec[0],rec[2]]
sorgente.close

fonte = "celle.csv"
sorgente = open(fonte)
reader = csv.reader(sorgente, delimiter=",")

for rec in reader:
    celle[rec[1]] = rec[0]
sorgente.close

lista = os.listdir('../creazione_tel_tipizz/extracted_data')

out = open("tel_cod.csv","w")
for l in lista:
    if l[-4:] != '.csv':
        continue
    ifile = open('extracted_data/'+l)
    print(l)
    reader = csv.reader(ifile, delimiter=';')
    for rec in reader:
        if rec[1] == 'data_ora':
            continue
        if rec[8][0] != '+':
            num = '+'+rec[8]
        else:
            num = rec[8]
        tel_da = telefoni[num][0]+str(telefoni[num][1])
        if rec[15][0] != '+':
            num = '+'+rec[15]
        else:
            num = rec[15]
        tel_a = telefoni[num][0]+str(telefoni[num][1])
        cel_da = ""
        if rec[4]:
        # torre chiamante
            if rec[4] in celle:
                cel_da = str(celle[rec[4]])
        cel_a = ""
        if rec[11]:
        # torre chiamata
            if rec[1] in celle:
                cel_a = str(celle[rec[11]])
        if "-" in rec[1]:
            ora = str(calendar.timegm(time.strptime(rec[1],"%Y-%m-%d %H:%M:%S")))
        else:
            ora = str(calendar.timegm(time.strptime(rec[1],"%d/%m/%Y %H:%M:%S")))
        out.write(ora+',"'+tel_da+'","'+tel_a+'","'+cel_da+'","'+cel_a+'"\r\n')

    ifile.close()

out.close()
