import csv
import os

fonte = "intercettati.csv"
sorgente = open(fonte)
reader = csv.reader(sorgente, delimiter=",")

intercettati = []
for rec in reader:
    intercettati.append(rec[0])
sorgente.close

lista = os.listdir('extracted_data')

telefoni = {}
celle = []
for l in lista:
    if l[-4:] != '.csv':
        continue
    ifile = open('extracted_data/'+l)
    print(l)
    reader = csv.reader(ifile, delimiter=';')
    for rec in reader:
        if rec[1] == 'data_ora':
            continue
        # controllo se il chiamante o il suo imei o il suo imsi sono intercettati
        # 8: numero chiamante
        if rec[8][0] != '+':
            num = '+'+rec[8]
        else:
            num = rec[8]
        if not num in telefoni:
            flag = 'N'
            if num in intercettati:
                flag = 'S'
            #9: imei chiamante
            elif bool(rec[9]):
                if '+'+rec[9] in intercettati:
                    flag = 'S'
            if flag == 'N':
                #10: imsi chiamante
                if bool(rec[10]):
                    if '+'+rec[10] in intercettati:
                        flag = 'S'
            telefoni[num] = flag
        # controllo se il chiamato o il suo imei o il suo imsi sono intercettati
        #15: numero chiamato
        if rec[15][0] != '+':
            num = '+'+rec[15]
        else:
            num = rec[15]
        if not num in telefoni:
            flag = 'N'
            if num in intercettati:
                flag = 'S'
            #16: imei chiamato
            elif bool(rec[16]):
                if '+'+rec[16] in intercettati:
                    flag = 'S'
            if flag == 'N':
                #17: imsi chiamato 
                if bool(rec[17]):
                    if '+'+rec[17] in intercettati:
                        flag = 'S'
            telefoni[num] = flag
        if bool(rec[4]):
        # torre chiamante
            if not rec[4] in celle:
                celle.append(rec[4])
        if bool(rec[11]):
        # torre chiamante
            if not rec[11] in celle:
                celle.append(rec[11])
    ifile.close()

tel=open("telefoni.csv","w")
tel.write("telefono,is_intercettato\n")
for t in telefoni:
    tel.write(f'"{t}","{telefoni[t]}"\n')
tel.close()

cel=open("celle.csv","w")
for t in celle:
    cel.write(t+'\r\n')
cel.close()
